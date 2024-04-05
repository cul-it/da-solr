package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.utilities.BoxInteractions.getBoxFileContents;
import static edu.cornell.library.integration.utilities.BoxInteractions.uploadFileToBox;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.AuthorityData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.Email;

public class ProcessAuthorityChangeFile {

	public static void main(String[] args) throws IOException, SQLException, SolrServerException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(        Config.getRequiredArgsForDB("Authority") );
		requiredArgs.add("blacklightUrl");
		requiredArgs.add("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredArgs);
		config.setDatabasePoolsize("Authority", 2);
		config.activateSES();

		Map<String, String> env = System.getenv();
		String fileId = env.get("box_file_id");
		String fileName = env.get("box_file_name");
		String requesterName = env.get("box_user_name");
		String requesterEmail = env.get("box_user_login");
		String testMode = env.get("test_mode");
		System.out.printf("file: %s ; %s\nrequester %s <%s>\n", fileId, fileName, requesterName, requesterEmail);

		if (testMode == null || ! testMode.equalsIgnoreCase("true"))
			checkForNewAuthorityFiles( config );

		String firstFile = null;
		String lastFile = null;
		String outputFile = null;

		String fileContent = getBoxFileContents(env.get("boxKeyFile"), fileId, fileName, 1024);

		String[] lines = fileContent.split("\\r?\\n");
		for (String line : lines ) {
			System.out.printf("[%s]\n",line);
		}
		if ( lines.length > 1 ) {
			firstFile = lines[0];
			lastFile = lines[1];
			outputFile = firstFile+"-"+lastFile.substring(lastFile.length()-2)+now()+".json";
		} else {
			firstFile = lines[0];
			lastFile = firstFile;
			outputFile = firstFile+now()+".json";
		}

		System.out.printf("Generating file %s\n", outputFile);
		String autoFlipFile = outputFile.replaceAll(".json", "-candidates.json");

		try ( Connection authority = config.getDatabaseConnection("Authority");
				HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl());
				PreparedStatement getAuthStmt = authority.prepareStatement(
						"SELECT *"+
						"  FROM authorityUpdate"+
						" WHERE updateFile BETWEEN ? AND ?"+
						" ORDER BY updateFile, positionInFile");
				BufferedWriter jsonWriter = Files.newBufferedWriter(Paths.get(outputFile));
				BufferedWriter autoFlipWriter = Files.newBufferedWriter(Paths.get(autoFlipFile))) {

			jsonWriter.append("[\n");
			autoFlipWriter.append("[\n");
			boolean writtenJson = false;
			boolean writtenAutoFlip = false;

			getAuthStmt.setString(1, firstFile);
			getAuthStmt.setString(2, lastFile);
			try (ResultSet records = getAuthStmt.executeQuery()) {
				while ( records.next() ) {

					Map<String,Object> json = new HashMap<>();

					MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY,records.getBytes("marc21"));
					ChangeType changeType = ChangeType.byOrdinal(records.getInt("changeType"));
					boolean lookForOldRecordVersion = changeType.equals(ChangeType.UPDATE);
					json.put("changeCategory",changeType.getDisplay());
					String id = records.getString("id");

					json.put("id", id);
					json.put("inputFile", records.getString("updateFile"));
					String mainEntry = records.getString("heading");
					json.put("heading",mainEntry);
					AuthoritySource vocab = AuthoritySource.byOrdinal(records.getInt("vocabulary"));

					System.out.println(id+": "+mainEntry);

					if ( records.getBoolean("undifferentiated") ) {
						json.put("undifferentiated", true);
						if ( changeType.equals(ChangeType.UPDATE) ) {
							System.out.println("Skipping undifferentiated record update. "+id);
							continue;
						}
					}

					MarcRecord oldR = null;
					if ( lookForOldRecordVersion )
						oldR = getOldRecordVersion(authority, id, records.getDate("moddate"), mainEntry, json);
					Map<String,Object> autoFlip = null;
					if (json.containsKey("oldHeading"))
						autoFlip = lookForEligibleAutoFlip(getHeadField(r), getHeadField(oldR));

					String differences = null;
					Map<String,EnumSet<DiffType>> actionableHeadings = null;
					if ( oldR != null ) {
						differences = compareOldAndNewMarc( oldR, r).toString() ;
						actionableHeadings = identifyActionableChanges( differences, r, mainEntry );
						if ( actionableHeadings.isEmpty() ) continue;
					} else {
						actionableHeadings = identifyActionableFields(
								r, changeType.equals(ChangeType.DELETE) );
					}

					if ( actionableHeadings.isEmpty() ) continue;
					json.put("actionableHeadings", serializeActionable(actionableHeadings));

					Set<String> checkedHeadings = new HashSet<>();
					String mainEntrySort = getFilingForm( mainEntry );
					List<Map<String,Object>> relevantChanges = new ArrayList<>();
					for (String field : actionableHeadings.keySet() ) {
						EnumSet<DiffType> flags = actionableHeadings.get(field);
						HeadingType ht = HeadingType.byAuthField("1"+field.substring(1, 3));
						String heading = field.substring(4);
						if ( checkedHeadings.contains(heading) ) continue;
						checkedHeadings.add(heading);
						String headingSort = getFilingForm(heading);
						if ( mainEntrySort.equals(headingSort))
							flags.add(DiffType.DIACR);

						boolean isSeparatelyAuthorized = false;
						edu.cornell.library.integration.metadata.support.HeadingType oldHt =
								ht.getOldHeadingType();
						if ( oldHt != null ) {
							AuthorityData auth = new AuthorityData(config,heading,oldHt);
							if (auth.authorized) for (String authId : auth.authorityId)
								if (! authId.trim().equals(id)
										&& isStillAuthorized(authority,headingSort,authId)) {
									isSeparatelyAuthorized = true;
									System.out.printf("Separately authorized: %s %s\n", authId,heading);
								}
						}

						List<String> searchFields = identifySearchFields(ht,vocab);
						if ( searchFields == null ) continue;

						for (String searchField : searchFields) {
							if ( ( flags.contains(DiffType.NEWMAIN) || isSeparatelyAuthorized)
									&& changeType.equals(ChangeType.UPDATE)
									&& ! ( searchField.contains("_unk_") ) ) continue;
							SolrQuery q = new SolrQuery(searchField+":\""+heading.replaceAll("\"","'")+'"');
							q.setRows(0);
							q.setFields("instance_id","id");

							SolrDocumentList res = solr.query(q).getResults();
							Long recordCount = res.getNumFound();
							if ( recordCount == 0 ) continue;
							if ( flags.contains(DiffType.DIACR) ) {
								String facetField = (searchField.startsWith("author")
										?"author_facet":searchField.replace("browse","facet"));
								Map<String,Long> displayForms = tabulateActualUnnormalizedHeadings(
										solr, heading, searchField, facetField);
								for ( String displayForm : displayForms.keySet() ) {
									if (displayForm.equals(mainEntry)) continue;
									System.out.printf("%s relevant w/ %d instances (%s).\n",
											displayForm,displayForms.get(displayForm),searchField);
									relevantChanges.add(buildRelevantChange(displayForm,searchField,facetField,
											displayForms.get(displayForm), flags, config, autoFlip, vocab));
								}
							} else {
								Map<String,Object> rc = buildRelevantChange(
										heading,searchField,searchField,recordCount, flags, config, autoFlip, vocab);
								relevantChanges.add(rc);
								if (rc.containsKey("autoFlip")) {
									q.setRows(recordCount.intValue()+100);
									res = solr.query(q).getResults();
									List<List<String>> instances = new ArrayList<>();
									for (SolrDocument doc : res) {
										instances.add(new ArrayList<String>(Arrays.asList(
												(String)doc.getFieldValue("id"),
												(String)doc.getFieldValue("instance_id"))));
									}
									autoFlip.put(searchField, instances);
								}
							}
						}
					}
					if ( ! relevantChanges.isEmpty() ) {
						json.put("relevantChanges", relevantChanges);
						if ( writtenJson ) jsonWriter.append(",\n"); else writtenJson = true;
						jsonWriter.append(mapper.writeValueAsString(json));
						if (autoFlip != null && autoFlip.keySet().size() > 3) {
							if ( writtenAutoFlip ) autoFlipWriter.append(",\n"); else writtenAutoFlip = true;
							autoFlipWriter.append(mapper.writeValueAsString(autoFlip));
						}
					}

				}
				jsonWriter.append("]");
				jsonWriter.flush();
				jsonWriter.close();
				autoFlipWriter.append("]");
				autoFlipWriter.flush();
				autoFlipWriter.close();

				if (testMode == null || ! testMode.equalsIgnoreCase("true")) {
					List<String> boxIds = uploadFileToBox(env.get("boxKeyFile"),"JSON folder",outputFile);
					registerReportCompletion(authority,config.getServerConfig("authReportEmail"),
							firstFile,lastFile, requesterName, requesterEmail,outputFile, boxIds);
					if (writtenAutoFlip) {
						boxIds = uploadFileToBox(env.get("boxKeyFile"),"Automatic Authority Flips",autoFlipFile);
					}
				}
			}
		}
	}

	private static Map<String,Object> lookForEligibleAutoFlip(DataField newHead, DataField oldHead) {

		// DATE CLOSURE FLIP
		DC: {
			if (! newHead.tag.equals("100")) break DC;
			if (! oldHead.tag.equals("100")) break DC;
			if (oldHead.subfields.size() != newHead.subfields.size()) break DC;
			Iterator<Subfield> oldI = oldHead.subfields.iterator();
			Iterator<Subfield> newI = newHead.subfields.iterator();
			boolean flippableD = false;
			while ( oldI.hasNext() ) {
				Subfield oldSF = oldI.next();
				Subfield newSF = newI.next();
				if ( ! oldSF.code.equals(newSF.code) ) break DC;
				if (oldSF.code.equals('d')) {
					if ( ! flippableDateChange( oldSF.value, newSF.value) ) break DC;
					flippableD = true;
				} else if (oldSF.code.equals('t') || oldSF.code.equals('k')) {
					break DC;
				} else {
					if ( ! getFilingForm(oldSF.value).equals( getFilingForm(newSF.value) )) break DC;
				}
			}
			if ( ! flippableD ) break DC;
			Map<String,Object> flip = new HashMap<>();
			flip.put("oldHeading",oldHead);
			flip.put("newHeading", newHead);
			flip.put("name", "DateClosure");
			System.out.printf("DateClosure: %s -> %s\n", oldHead.toString(), newHead.toString());
			return flip;
		}

		return null;
	}

	static boolean flippableDateChange(String d1, String d2) {
		if (d1.equals(d2)) return false;
		d1 = d1.replaceAll("[,.]*$", "");
		d2 = d2.replaceAll("[,.]*$", "");
		if (d1.equals(d2)) return false;
		String d1norm = normalizeDates(d1);
		String d2norm = normalizeDates(d2);
		if (d1norm.equals(d2norm)) return true;
		String[] d1parts = d1norm.split("-",-1);
		String[] d2parts = d2norm.split("-",-1);
		if ( d1parts.length == 2 && d2parts.length == 2) {
			if (d1parts[0].equals(d2parts[0]) && d1parts[1].isBlank())
				return true;
			if (d1parts[1].equals(d2parts[1]) && d1parts[0].isBlank())
				return true;
			Integer leftDiff = yearsDiff(d1parts[0], d2parts[0]);
			Integer rightDiff = yearsDiff(d1parts[1], d2parts[1]);
			if (leftDiff == null || rightDiff == null) return false;
			if (leftDiff == 0 && rightDiff < 5) return true;
			if (rightDiff == 0 && leftDiff < 5) return true;
		}
		return false;
	}

	private static Integer yearsDiff(String y1, String y2) {
		if (y1 == null || y1.isBlank()) return null;
		if (y2 == null || y2.isBlank()) return null;
		try {
			int year1 = Integer.valueOf( y1.replaceAll("[^\\d]", "") );
			int year2 = Integer.valueOf( y2.replaceAll("[^\\d]", "") );
			if (year1 < 1000 || year1 > 9999) return null;
			if (year2 < 1000 || year2 > 9999) return null;
			return Math.abs( year1 - year2 );
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String normalizeDates(String before) {
		return before
				.replaceAll("\\bca\\. ?", "approximately ")
				.replaceAll("\\bfl\\. ?", "active ")
				.replaceAll("\\bcent(\\.|\\b)", "century")
				.replaceAll("\\bb\\. ?([^\\-]+)", "$1-")
				.replaceAll("\\bd\\. ?(\\d\\d\\d\\d)", "-$1")
				;
	}

	private static DataField getHeadField(MarcRecord r) {
		for (DataField f : r.dataFields)
			if (f.tag.startsWith("1"))
				return f;
		return null;
	}

	private static MarcRecord getOldRecordVersion(Connection authority, String id,
			java.sql.Date moddate, String mainEntry, Map<String,Object> json) throws SQLException {
		MarcRecord oldR = null;
		try (PreparedStatement getOldRecordStmt = authority.prepareStatement(
				"SELECT marc21 FROM authorityUpdate WHERE id = ? AND moddate < ? ORDER BY moddate DESC")){
			getOldRecordStmt.setString(1, id);
			getOldRecordStmt.setDate(2, moddate);
			try ( ResultSet rs = getOldRecordStmt.executeQuery() ) {
				while (rs.next()) {
					byte[] marc = rs.getBytes(1);
					try {
						oldR = new MarcRecord(MarcRecord.RecordType.AUTHORITY,marc);
						String oldMainEntry = null;
						for ( DataField f : oldR.dataFields ) if ( f.tag.startsWith("1") ) {
							if ( f.tag.startsWith("18") )
								oldMainEntry = f.concatenateSpecificSubfields(" > ", "vxyz");
							else {
								oldMainEntry = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
								String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
								if ( ! oldMainEntry.isEmpty() && ! dashed_terms.isEmpty() )
									oldMainEntry += " > "+dashed_terms;
							}
						}
						if ( ! mainEntry.equals(oldMainEntry) )
							json.put("oldHeading", oldMainEntry);
						return oldR;
					} catch(IllegalArgumentException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return null;
	}

	private static void registerReportCompletion(
			Connection db, Map<String,String> authReportEmailConfig, String firstFile, String lastFile,
			String toName, String toEmail, String file, List<String> boxIds)
			throws SQLException {
		try (PreparedStatement stmt = db.prepareStatement(
						"UPDATE lcAuthorityFile SET reportedDate = NOW() WHERE updateFile BETWEEN ? AND ?")){
			stmt.setString(1, firstFile);
			stmt.setString(2, lastFile);
			stmt.executeUpdate();
		}
		String fullSubjectLine = String.format("%s: %s", authReportEmailConfig.get("Subject"), file);
		System.out.printf("Sending email to %s <%s>\n", toName, toEmail);

		StringBuilder msgBuilder = new StringBuilder();
		msgBuilder.append("<html>\n");
		msgBuilder.append(String.format("<p>File %s available at:<br/>\n", file));
		msgBuilder.append(String.format("file: https://cornell.app.box.com/file/%s<br/>\n",boxIds.get(0)));
		msgBuilder.append(String.format("folder: https://cornell.app.box.com/folder/%s</p>\n\n", boxIds.get(1)));

		try (PreparedStatement s = db.prepareStatement(
				"SELECT file.updateFile, file.postedDate, file.importedDate, COUNT(*) "+
				"  FROM lcAuthorityFile file, authorityUpdate upd "+
				" WHERE file.updateFile = upd.updateFile AND reportedDate IS NULL"+
				" GROUP BY file.updateFile ORDER BY 1");
				ResultSet rs = s.executeQuery()) {

			msgBuilder.append("<table border=2><tr><th colspan=4>FILES AVAILABLE FOR REPORTING</th></tr>\n");
			msgBuilder.append("<tr><th>FILE</th><th>POSTED</th><th>IMPORTED</th><th>AUTHRECS</th></tr>\n");

			while (rs.next())
				msgBuilder.append(String.format("<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td></tr>", 
						rs.getString(1), humanDate(rs.getDate(2)), humanDate(rs.getDate(3)), rs.getInt(4)));

			msgBuilder.append("</table></html>");
		}
		Email.sendSESHtmlMessage(
				authReportEmailConfig.get("From"),
				String.format("%s <%s>", toName, toEmail),
				fullSubjectLine,
				msgBuilder.toString());
	}

	private static String now(  ) {
		return nowFormat.format( new Date() );
	}
	private static String humanDate( Date date ) {
		if (date == null) return "";
		return dateFormat.format( date );
	}
	private static SimpleDateFormat nowFormat = new SimpleDateFormat("_yyMMdd-kkmm");
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");

	private static void checkForNewAuthorityFiles(Config config) throws SQLException{

		try (Connection authdb = config.getDatabaseConnection("Authority");
				Statement stmt = authdb.createStatement();
				){
			int updateCount = stmt.executeUpdate(
					"UPDATE updateCursor SET current_to_date = NOW()"+
					" WHERE cursor_name = 'lcAuthUpda'"+
					"   AND current_to_date < DATE_SUB(NOW(), INTERVAL '1' HOUR)");
			if ( updateCount == 0 ) {
				System.out.println("Not checking FTP server for new LC auth updates twice in an hour.");
				return;
			}
			RetrieveAuthorityUpdateFiles.checkLCAuthoritiesFTP(config);
		}
	}

	private static boolean isStillAuthorized(
			Connection authority, String headingSort, String authId) throws SQLException {
		try (PreparedStatement stmt = authority.prepareStatement(
				"SELECT heading, changeType"+
				"  FROM authorityUpdate"+
				" WHERE id = ?"+
				" ORDER BY moddate DESC"+
				" LIMIT 1")) {
			stmt.setString(1, authId);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					if (rs.getInt(2) == ChangeType.DELETE.ordinal()) {
						System.out.printf("Authority deleted: %s\n", authId);
						return false;
					}
					if ( ! headingSort.equals(getFilingForm(rs.getString(1)))) {
						System.out.printf("Authority heading no longer matches: %s\n%s\n%s\n",
								authId,headingSort,getFilingForm(rs.getString(1)));
						return false;
					}
				}
			}
		}
		return true;
	}

	//	static Pattern dateMatcher = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2}).*");
	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }

	private static String serializeActionable(Map<String, EnumSet<DiffType>> actionableHeadings) {
		StringBuilder sb = new StringBuilder();
		for (String heading : actionableHeadings.keySet()) {
			sb.append(heading);
			EnumSet<DiffType> flags = actionableHeadings.get(heading);
			if ( ! flags.isEmpty() )
				sb.append(": "+flags.toString());
			sb.append("\n");
		}
		return sb.toString();
	}

	private static Map<String,EnumSet<DiffType>> identifyActionableFields(
			MarcRecord r, boolean isDelete) {
		Map<String,EnumSet<DiffType>> fields = new HashMap<>();
		for (DataField f : r.dataFields) {
			if ( ! f.tag.startsWith("1") && ! f.tag.startsWith("4") && ! f.tag.startsWith("78")) continue;
			String heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
			String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
			EnumSet<DiffType> flags = EnumSet.noneOf(DiffType.class);
			if (f.tag.startsWith("1") && ! isDelete) flags.add(DiffType.NEWMAIN);
			if ( heading.isEmpty() ) {
				if ( ! dashed_terms.isEmpty() ) fields.put(f.tag+" "+dashed_terms,flags);
			} else {
				if ( ! dashed_terms.isEmpty() ) heading += " > "+dashed_terms;
				fields.put(f.tag+" "+heading,flags);
			}
		}
		return fields;
	}

	private static Map<String,EnumSet<DiffType>> identifyActionableChanges(
			String differences, MarcRecord r, String mainHeading) {

		Map<String,EnumSet<DiffType>> headings = new HashMap<>();
		for ( String difference : differences.split("\n")) {
			String tag = difference.substring(2, 5);
			boolean isNew = difference.startsWith("+");
			if ( ! tag.startsWith("1") && ! tag.startsWith("4") && ! tag.startsWith("78")) continue;
			boolean newMain = tag.startsWith("1") && isNew;
			DataField f = new DataField(1,tag,difference.charAt(5),
					difference.charAt(6),difference.substring(6));
			Map<String,EnumSet<DiffType>> diffHeadings = findHeadingVariants(f,newMain, mainHeading);
			for ( Entry<String,EnumSet<DiffType>> e : diffHeadings.entrySet() ) {
				e.getValue().add((isNew)?DiffType.NEW:DiffType.OLD);
				headings.putIfAbsent(e.getKey(),e.getValue());
			}
		}
		// pull in all the 4xx headings for updated records
		for ( DataField f : r.dataFields ) if ( f.tag.startsWith("4") ) {
			Map<String,EnumSet<DiffType>> diffHeadings = findHeadingVariants(f,false, mainHeading);
			for ( Entry<String,EnumSet<DiffType>> e : diffHeadings.entrySet() ) {
				e.getValue().add(DiffType.UNCH);
				headings.putIfAbsent(e.getKey(),e.getValue());
			}
		}
		return headings;
	}

	private static Entry<String, EnumSet<DiffType>> headingOf(
			String mainHead, DataField f, DiffType variantType, boolean newMain ) {
		String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
		String heading = null;
		if ( mainHead.isEmpty() ) {
			if ( ! dashed_terms.isEmpty() )
				heading = f.tag + " " + dashed_terms;
			else
				return null;
		}
		if ( ! dashed_terms.isEmpty() )
			heading = f.tag + " " + mainHead + " > " + dashed_terms;
		else
			heading = f.tag + " " + mainHead;
		EnumSet<DiffType> flags = (variantType == null)
				?EnumSet.noneOf(DiffType.class):EnumSet.of(variantType);
		if ( newMain ) flags.add(DiffType.NEWMAIN);
		if (heading.endsWith(" ~~~"))
			heading = heading.replaceAll(" ~~~", "");

		return new AbstractMap.SimpleEntry<>(heading,flags);
	}

	private static Map<String, EnumSet<DiffType>> findHeadingVariants(
			DataField f,boolean newMain, String preferredHeading) {

		String preferredClean = removeTrailingPunctuation(preferredHeading,"., ");

		Map<String,EnumSet<DiffType>> headings = new HashMap<>();
		String mainHead = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
		Entry<String,EnumSet<DiffType>> mainEntry = headingOf(mainHead,f,null,newMain);
		headings.putIfAbsent(mainEntry.getKey(), mainEntry.getValue());

		if ( ! mainHead.isEmpty() ) {
			String mainHead_sansQ = f.concatenateSpecificSubfields("abcdefghjklmnoprstu");
			if ( ! mainHead_sansQ.equals(mainHead) && ! mainHead_sansQ.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansQ,f,DiffType.VAR_Q,false);
				String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
				if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
								extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
					headings.putIfAbsent(entry.getKey(), entry.getValue());
			}

			String mainHead_sansD = f.concatenateSpecificSubfields("abcefghjklmnopqrstu");
			if ( ! mainHead_sansD.equals(mainHead) && ! mainHead_sansD.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansD,f,DiffType.VAR_D,false);
				String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
				if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
								extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
					headings.putIfAbsent(entry.getKey(), entry.getValue());
			}

			String mainHead_sansQD = f.concatenateSpecificSubfields("abcefghjklmnoprstu");
			if ( ! mainHead_sansQD.equals(mainHead) && ! mainHead_sansQD.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansQD,f,DiffType.VAR_QD,false);
				String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
				if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
								extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
					headings.putIfAbsent(entry.getKey(), entry.getValue());
			}
		}

		return headings;
	}
	private enum DiffType {
		NEWMAIN, DIACR, UNCH, VAR_Q, VAR_D, VAR_QD, OLD, NEW;
	}

	private static Map<String,Long> tabulateActualUnnormalizedHeadings (
			HttpSolrClient solr, String heading, String field, String facetField)
					throws SolrServerException, IOException {

		Map<String,Boolean> authors = new HashMap<>();
		Map<String,Long> displayForms = new HashMap<>();
		String normalizedHeading = getFilingForm( heading );

		SolrQuery q = new SolrQuery(field+":\""+heading.replaceAll("\"","'")+'"');
		q.setRows(10_000);
		q.setFields(facetField,"id");

		boolean incompleteIndexing = false;
		for (SolrDocument doc : solr.query(q).getResults()) {
			if ( ! doc.containsKey(facetField)) {
				System.out.printf("Needs indexing: %s\n", doc.getFieldValue("id"));
				incompleteIndexing = true;
			} else for (String author : (ArrayList<String>)doc.getFieldValue(facetField)) {
				if ( ! authors.containsKey(author) )
					authors.put(author, normalizedHeading.equals(getFilingForm(author)));
				if ( ! authors.get(author) )
					continue; 
				if ( displayForms.containsKey(author) )
					displayForms.put(author, displayForms.get(author)+1);
				else displayForms.put(author,1L);
			}
		}
		if ( incompleteIndexing ) {
			System.out.printf( "query: %s\nfacet %s\n",q.getQuery(),facetField);
		}

		for ( String form : displayForms.keySet() )
			System.out.printf("%s: %d\n", form, displayForms.get(form));
		return displayForms;
	}

	private static List<String> identifySearchFields(HeadingType fieldType, AuthoritySource vocab) {
		List<String> searchFields = new ArrayList<>();
		switch (fieldType) {
		case PERS:
			searchFields.add("author_pers_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_pers_lcjsh_browse");
			else {
				searchFields.add("subject_pers_lc_browse");
				searchFields.add("subject_pers_unk_browse");
			}
			break;
		case CORP:
			searchFields.add("author_corp_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_corp_lcjsh_browse");
			else {
				searchFields.add("subject_corp_lc_browse");
				searchFields.add("subject_corp_unk_browse");
			}
			break;
		case MEETING:
		case EVENT:
			searchFields.add("author_event_roman_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_event_lcjsh_browse");
			else {
				searchFields.add("subject_event_lc_browse");
				searchFields.add("subject_event_unk_browse");
			}
			break;
		case WORK:
			searchFields.add("title_exact");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_work_lcjsh_browse");
			else {
				searchFields.add("subject_work_lc_browse");
				searchFields.add("subject_work_unk_browse");
			}
			break;
		case ERA:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_era_lcjsh_browse");
			else {
				searchFields.add("subject_era_lc_browse");
				searchFields.add("subject_era_unk_browse");
			}
			break;
		case TOPIC:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_topic_lcjsh_browse");
			else {
				searchFields.add("subject_topic_lc_browse");
				searchFields.add("subject_topic_unk_browse");
			}
			break;
		case PLACE:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_geo_lcjsh_browse");
			else {
				searchFields.add("subject_geo_lc_browse");
				searchFields.add("subject_geo_unk_browse");
			}
			break;
		case GENRE:
			searchFields.add("subject_genr_lcgft_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_genr_lcjsh_browse");
			else {
				searchFields.add("subject_genr_lc_browse");
				searchFields.add("subject_genr_unk_browse");
			}
			break;
		case INSTRUMENT:
			searchFields = Arrays.asList("notes_t");
			break;
		case SUB_GEN:
		case SUB_GEO:
		case SUB_ERA:
		case SUB_GNR:
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_sub_lcjsh_browse");
			else {
				searchFields.add("subject_sub_lc_browse");
				searchFields.add("subject_sub_unk_browse");
			}
			break;
		default:
			System.out.println("Unexpected field type "+fieldType);
			return null;
		}
		return searchFields;
	}

	private static Map<String,Object> buildRelevantChange (
			String heading, String searchField, String blField, Long records, EnumSet<DiffType> flags,
			Config config, Map<String,Object> autoFlip, AuthoritySource vocab)
					throws UnsupportedEncodingException {

		Map<String,Object> rc = new HashMap<>();
		rc.put("heading", heading);
		String link;
		if ( blField.endsWith("facet") )
			link = config.getBlacklightUrl()+"/?f["+blField+"][]="+heading;
		else
			link = config.getBlacklightUrl()+"/?q=%22"+
					URLEncoder.encode(heading, "UTF-8")+"%22&search_field="+blField;
		rc.put("blacklightLink", link);
		String solrLink =
				config.getBlacklightSolrUrl()+"/select?"
				+ "qt=search&wt=csv&rows=9999999&fl=instance_id&q="
				+ blField+":%22"+URLEncoder.encode(heading, "UTF-8")+"%22";

		if      (blField.contains("_lc_"))    rc.put("vocab", "lc");
		else if (blField.contains("_unk_"))   rc.put("vocab", "unk");
		else if (blField.contains("_lcgft_")) rc.put("vocab", "lcgft");
		else if (blField.contains("_lcjsh_")) rc.put("vocab", "lcjsh");
		else if (blField.contains("_other_")) rc.put("vocab", "other");
		else if (blField.contains("_fast_"))  rc.put("vocab", "fast");

		if      (searchField.contains("_pers_"))  rc.put("type", "pers");
		else if (searchField.contains("_corp_"))  rc.put("type", "corp");
		else if (searchField.contains("_event_")) rc.put("type", "event");
		else if (searchField.contains("_work_"))  rc.put("type", "work");
		else if (searchField.contains("_era_"))   rc.put("type", "era");
		else if (searchField.contains("_topic_")) rc.put("type", "topic");
		else if (searchField.contains("_geo_"))   rc.put("type", "geo");
		else if (searchField.contains("_gen_"))   rc.put("type", "gen");
		else if (searchField.contains("_genr_"))  rc.put("type", "genr");
		else if (searchField.contains("_sub_"))   rc.put("type", "sub");

		rc.put("solrLink", solrLink);
		rc.put("instanceCount",records);
		if (flags.contains(DiffType.NEWMAIN))
			rc.put("newMainHeading", true);
		if (flags.contains(DiffType.DIACR))
			rc.put("diacriticsPunctuationCapitalization", true);
		if (flags.contains(DiffType.VAR_D))
			rc.put("variantHeadingType", "No $d");
		if (flags.contains(DiffType.VAR_Q))
			rc.put("variantHeadingType", "No $q");
		if (flags.contains(DiffType.VAR_QD))
			rc.put("variantHeadingType", "No $q or $d");
		if (autoFlip != null
				&& flags.contains(DiffType.OLD) && flags.size() == 1
				&& (blField.contains("author") || rc.get("vocab").equals("lc")))
			rc.put("autoFlip", autoFlip.get("name"));

		return rc;
	}

	private static LinkedHashMap<String,String> serializeForComparison(MarcRecord marc) {
/*		marc.leader = "00000" + marc.leader.substring(5, 12) + "00000" + marc.leader.substring(17);
		ControlField five = null;
		for (ControlField f : marc.controlFields)
			if (f.tag.equals("005"))
				five = f;
			else if ( f.tag.equals("008") && f.value.length() > 6 )
				f.value = "000000"+f.value.substring(6);
		if (five != null)
			marc.controlFields.remove(five);*/
		for ( DataField f : marc.dataFields ) if ( f.tag.startsWith("1") ) f.ind2 = ' ';
		String recordAsString = " "+Normalizer.normalize(
				marc.toString(), Normalizer.Form.NFC).replaceAll("\u0361(.)", "\uFE20$1\uFE21");
		List<String> lines = Arrays.asList(recordAsString.split("\n"));
		LinkedHashMap<String,String> serializedFields = new LinkedHashMap<>();
		for (String line : lines ) {
			if ( line.startsWith("1") ) line = removeTrailingPunctuation(line,". ");
			String sortableLine = getFilingForm(line);
			serializedFields.put(line, sortableLine);
		}
		return serializedFields;
	}
	private static StringBuilder compareOldAndNewMarc(MarcRecord rec1, MarcRecord rec2) {
		Map<String,String> before = serializeForComparison(rec1);
		Map<String,String> after = serializeForComparison(rec2);
		List<String> common = new ArrayList<>();
		List<String> commonSortables = new ArrayList<>();
		for (String b : before.keySet())
			for (String a : after.keySet())
				if (b.equals(a)) {
					common.add(b);
				} else if (before.get(b).equals(after.get(a)))
					commonSortables.add(before.get(b));
		Map<String, Boolean> diff = new TreeMap<>();
		for (String b : before.keySet())
			if (!common.contains(b)) {
				if (! commonSortables.contains(before.get(b)))
					diff.put(b, false);
				else 
					diff.put(b+" ~~~", false);
			}
		for (String a : after.keySet())
			if (!common.contains(a))
				if (! commonSortables.contains(after.get(a)))
					diff.put(a, true);
				else 
					diff.put(a+" ~~~", true);
		if (diff.isEmpty())
			return new StringBuilder();
		StringBuilder sb = new StringBuilder();
		for (Entry<String, Boolean> e : diff.entrySet())
			sb.append((e.getValue()) ? "+ " : "- ").append(e.getKey()).append('\n');
		return sb;
	}

	
	public static byte[] readFile(String filename) throws IOException {
		return Files.readAllBytes(Paths.get(filename));
	}

}

/* LOAD FROM VOYAGER ORACLE DATABASE
 *  (Leaving the code here for reference)

Collection<String> requiredArgs = Config.getRequiredArgsForDB("Voy");
Config config = Config.loadConfig(requiredArgs);
config.setDatabasePoolsize("Voy", 2);
Set<Integer> voyIds = new TreeSet<>();
DownloadMARC download = new edu.cornell.library.integration.voyager.DownloadMARC();
download.setConfig(config);

try ( Connection voyager = config.getDatabaseConnection("Voy");
		Statement stmt = voyager.createStatement();
		Connection inventory = config.getDatabaseConnection("Current");
		PreparedStatement insertStmt = inventory.prepareStatement(
				"INSERT INTO authority.voyagerAuthority (id,voyId,marc21,marcxml,human,moddate) "+
				"VALUES (?,?,?,?,?,?)")) {
	stmt.setFetchSize(1_000_000);
	int count = 0;
	try ( ResultSet rs = stmt.executeQuery(
			"SELECT auth_id FROM auth_master WHERE auth_id > 4188280")){
		while ( rs.next() ) voyIds.add(rs.getInt(1));
		for ( Integer voyId : voyIds ) {
			byte[] marcBytes = download.downloadMrc(MarcRecord.RecordType.AUTHORITY, voyId);
			MarcRecord rec = new MarcRecord( MarcRecord.RecordType.AUTHORITY, marcBytes);
			String id = null;
			Timestamp moddate = null;
			if ( ++count % 10_000 == 0 ) System.out.println(count+": "+voyId);
			for ( ControlField f : rec.controlFields )
				if ( f.tag.equals("005") ) {
					Matcher m = dateMatcher.matcher(f.value);
					if ( m.matches() ) {
						String timestamp = String.format("%s-%s-%s %s:%s:%s",
								m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6));
						moddate = Timestamp.valueOf(timestamp);
					}
				}
			for ( DataField f : rec.dataFields ) if ( f.tag.equals("010") )
				for ( Subfield sf : f.subfields ) if ( sf.code.equals('a') )
					id = sf.value.trim();
			if ( id == null ) {
				System.out.println("No authority identifier for record.");
				System.out.println(rec.toString());
				continue;
			}
			insertStmt.setString(1, id);
			insertStmt.setInt(2, voyId);
			insertStmt.setString(3, new String( marcBytes, StandardCharsets.UTF_8));
			insertStmt.setString(4, rec.toXML());
			insertStmt.setString(5, rec.toString());
			insertStmt.setTimestamp(6, moddate);
			insertStmt.executeUpdate();
		}
	}
}
System.out.println(voyIds.size());
*/
