package edu.cornell.library.integration.authority;

import static edu.cornell.library.integration.authority.Solr.identifySearchFields;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibCount;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibs;
import static edu.cornell.library.integration.authority.Solr.tabulateActualUnnormalizedHeadings;
import static edu.cornell.library.integration.utilities.BoxInteractions.getBoxFileContents;
import static edu.cornell.library.integration.utilities.BoxInteractions.uploadFileToBox;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.AuthorityData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.Email;

public class ProcessAuthorityChangeFile {

	public static void main(String[] args) throws IOException, SQLException, SolrServerException, XMLStreamException {

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
		System.out.printf("file: %s ; %s\nrequester %s <%s>\n", fileId, fileName, requesterName, requesterEmail);

		Map<String,String> authConfig = config.getServerConfig("authReport");
		if ( ! authConfig.containsKey("Mode") || ! authConfig.get("Mode").equalsIgnoreCase("test"))
			checkForNewAuthorityFiles( config );

		String firstFile = null;
		String lastFile = null;
		String outputFile = null;
		String aspaceOutputFile = null;

		String fileContent = new String(
				getBoxFileContents(env.get("boxKeyFile"), fileId, fileName, 1024),
				StandardCharsets.UTF_8);

		String[] lines = fileContent.split("\\r?\\n");
		for (String line : lines ) {
			System.out.printf("[%s]\n",line);
		}
		if ( lines.length > 1 ) {
			firstFile = lines[0];
			lastFile = lines[1];
			outputFile = firstFile+"-"+lastFile.substring(lastFile.length()-2)+".json";
			aspaceOutputFile = firstFile+"-"+lastFile.substring(lastFile.length()-2)+"-aspace.json";
		} else {
			firstFile = lines[0];
			lastFile = firstFile;
			outputFile = firstFile+".json";
			aspaceOutputFile = firstFile+"-aspace.json";
		}

		System.out.printf("Generating file %s\n", outputFile);
		String autoFlipFile = outputFile.replaceAll(".json", "-candidates.json");

		try ( Connection authority = config.getDatabaseConnection("Authority");
				Http2SolrClient solr = new Http2SolrClient
						.Builder(config.getBlacklightSolrUrl())
						.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();
				PreparedStatement getAuthStmt = authority.prepareStatement(
						"SELECT *"+
						"  FROM authorityUpdate"+
						" WHERE updateFile BETWEEN ? AND ?"+
						" ORDER BY updateFile, positionInFile");
				BufferedWriter jsonWriter = Files.newBufferedWriter(Paths.get(outputFile));
				BufferedWriter jsonASpaceWriter = Files.newBufferedWriter(Paths.get(aspaceOutputFile));
				BufferedWriter autoFlipWriter = Files.newBufferedWriter(Paths.get(autoFlipFile))) {

			jsonWriter.append("[\n");
			jsonASpaceWriter.append("[\n");
			autoFlipWriter.append("[\n");
			boolean writtenJson = false;
			boolean writtenASpaceJson = false;
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
					String updateFile = records.getString("updateFile");
					json.put("inputFile", updateFile);
					String mainEntry = records.getString("heading").replaceAll("\\\\$", "");
					json.put("heading",mainEntry);
					AuthoritySource vocab = AuthoritySource.byOrdinal(records.getInt("vocabulary"));
					DataField mainEntryField = getHeadField(r);
					boolean looksLikeAACR2 = looksLikeAACR2(mainEntryField);

					System.out.format("[%s/%d] %s: %s\n",updateFile.replaceAll("[a-z]",""),
							records.getInt("positionInFile"), id, mainEntry);

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
					String mostRecentHeading = getMostRecentHeadingVersion(authority, id);

					Map<String,Change> actionableHeadings = null;
					if ( oldR != null ) {
						Map<String,DataField> differences = compareOldAndNewMarc( oldR, r);
						actionableHeadings = identifyActionableChanges(
								differences, r, mainEntryField, mainEntry, mostRecentHeading );
					} else {
						actionableHeadings = identifyActionableFields(
								r, changeType.equals(ChangeType.DELETE), mostRecentHeading );
					}

					if ( actionableHeadings.isEmpty() ) continue;
					json.put("actionableHeadings", serializeActionable(actionableHeadings));

					Set<String> checkedHeadings = new HashSet<>();
					String mainEntrySort = getFilingForm( mainEntry );
					List<Map<String,Object>> relevantChanges = new ArrayList<>();
					List<Map<String,Object>> relevantASpaceChanges = new ArrayList<>();
					for (String field : actionableHeadings.keySet() ) {
						EnumSet<DiffType> flags = actionableHeadings.get(field).flags;
						if ( flags.contains(DiffType.CURRENT) ) continue;
						Map<String,Object> autoFlip = actionableHeadings.get(field).autoFlip;
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

						List<String> searchFields = identifySearchFields(ht,vocab, autoFlip != null);
						if ( searchFields == null ) continue;

						for (String searchField : searchFields) {
							if ( ( flags.contains(DiffType.NEWMAIN) || isSeparatelyAuthorized)
									&& changeType.equals(ChangeType.UPDATE)
									&& ! ( searchField.contains("_unk_") ) ) continue;
//							SolrQuery q = new SolrQuery(searchField+":\""+heading.replaceAll("\"","'")+'"');
//							q.setRows(0);
//							q.setFields("instance_id","id");
//
//							SolrDocumentList res = solr.query(q).getResults();
//							Long recordCount = res.getNumFound();
							int recordCount = querySolrForMatchingBibCount(solr,searchField,heading,false);
							int aspaceCount = (recordCount > 0)
									? querySolrForMatchingBibCount(solr,searchField,heading,true) : 0;
							if ( 0 == recordCount) continue;
							if ( flags.contains(DiffType.DIACR) ) {
								String facetField = (searchField.startsWith("author")
										?"author_facet":searchField.replace("browse","facet"));
								Map<String,Integer> displayForms = tabulateActualUnnormalizedHeadings(
										solr, heading, searchField, facetField, false);
								for ( String displayForm : displayForms.keySet() ) {
									if (displayForm.equals(mainEntry)) continue;
									relevantChanges.add(buildRelevantChange(displayForm,searchField,facetField,
											displayForms.get(displayForm), flags, config, autoFlip, false, looksLikeAACR2));
								}
								if ( 0 == aspaceCount) continue;
								displayForms = tabulateActualUnnormalizedHeadings(
										solr, heading, searchField, facetField, true);
								for ( String displayForm : displayForms.keySet() ) {
									if (displayForm.equals(mainEntry)) continue;
									relevantASpaceChanges.add(buildRelevantChange(displayForm,searchField,facetField,
											displayForms.get(displayForm), flags, config, autoFlip, true, looksLikeAACR2));
								}
							} else {
								Map<String,Object> rc = buildRelevantChange(
										heading,searchField,searchField,recordCount, flags, config, autoFlip, false, looksLikeAACR2);
								relevantChanges.add(rc);
								if (rc.containsKey("autoFlip"))
									autoFlip.put(searchField, querySolrForMatchingBibs(solr,searchField,heading));
								if ( 0 < aspaceCount) relevantASpaceChanges.add(buildRelevantChange(
										heading,searchField,searchField,aspaceCount, flags, config, autoFlip, true, looksLikeAACR2));
							}
						}
						if (autoFlip != null && autoFlip.keySet().size() > 3) {
							if ( writtenAutoFlip ) autoFlipWriter.append(",\n"); else writtenAutoFlip = true;
							autoFlip.put("authorityId", id);
							autoFlipWriter.append(mapper.writeValueAsString(autoFlip));
						}
					}
					if ( ! relevantChanges.isEmpty() ) {
						json.put("relevantChanges", relevantChanges);
						if ( writtenJson ) jsonWriter.append(",\n"); else writtenJson = true;
						jsonWriter.append(mapper.writeValueAsString(json));
					}
					if ( ! relevantASpaceChanges.isEmpty() ) {
						json.put("relevantChanges", relevantASpaceChanges);
						if ( writtenASpaceJson ) jsonASpaceWriter.append(",\n"); else writtenASpaceJson = true;
						jsonASpaceWriter.append(mapper.writeValueAsString(json));
					}

				}
				jsonWriter.append("]");
				jsonWriter.flush();
				jsonWriter.close();
				jsonASpaceWriter.append("]");
				jsonASpaceWriter.flush();
				jsonASpaceWriter.close();
				autoFlipWriter.append("]");
				autoFlipWriter.flush();
				autoFlipWriter.close();

				List<String> boxIds = uploadFileToBox(env.get("boxKeyFile"),authConfig.get("OutputDir"),outputFile);
				uploadFileToBox(env.get("boxKeyFile"),authConfig.get("OutputDir"),aspaceOutputFile);
				registerReportCompletion(
						authority,authConfig,firstFile,lastFile, requesterName, requesterEmail,outputFile, boxIds);
				if (writtenAutoFlip) {
					boxIds = uploadFileToBox(env.get("boxKeyFile"),authConfig.get("AutoFlipDir"),autoFlipFile);
					triggerFlipJob(config, autoFlipFile);
				}
			}
		}
	}

	private static void triggerFlipJob(Config config, String inputFile) throws IOException {
		Map<String,String> prefectConfig = config.getServerConfig("prefect");
		Map<String,Object> payload = new HashMap<>();
		payload.put("state", Map.of("type","SCHEDULED"));
		payload.put("parameters", Map.of(
				"config_block", prefectConfig.get("FlipJobConfig"),
				"input_file",   inputFile));
		payload.put("idempotency_key", java.util.UUID.randomUUID());
		String url = String.format("%s/accounts/%s/workspaces/%s/deployments/%s/create_flow_run",
				prefectConfig.get("Api"), prefectConfig.get("Account"),
				prefectConfig.get("Workspace"), prefectConfig.get("FlipJobDeployment"));
		final HttpURLConnection c = (HttpURLConnection) (new URL(url)).openConnection();
		c.setRequestProperty("Content-type", "application/json; charset=utf-8");
		c.setRequestProperty("Authorization", "Bearer "+prefectConfig.get("Token"));
		c.setRequestMethod("POST");
		c.setDoOutput(true);
		final OutputStreamWriter writer = new OutputStreamWriter(c.getOutputStream());
		writer.write(mapper.writeValueAsString(payload));
		writer.flush();
		writer.close();
		c.connect();
		if (c.getResponseCode() == 201)
			System.out.println("Auto-flip job launched");
		else
			System.out.format("Auto-flip job launch unsuccessful. %d: %s\n", c.getResponseCode(), c.getResponseMessage());
	}

	private static String getMostRecentHeadingVersion(Connection authority, String id) throws SQLException {
		try( PreparedStatement mostRecentHeadingStmt = authority.prepareStatement(
				"SELECT heading FROM authorityUpdate WHERE id = ? ORDER BY moddate DESC LIMIT 1")) {
			mostRecentHeadingStmt.setString(1, id);
			try (ResultSet rs = mostRecentHeadingStmt.executeQuery()) {
				while (rs.next()) return rs.getString(1);
			}
		}
		return null;
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
			if (leftDiff == 0 && rightDiff == 0) return true;
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

	private static boolean looksLikeAACR2(DataField nameField) {
		if ( ! nameField.mainTag.endsWith("00") ) return false;
		for (Subfield sf : nameField.subfields) if (sf.code.equals('d')) {
			Matcher m = aacr2DateMarkerPattern.matcher(sf.value);
			return m.matches();
		}
		return false;
	}
	static Pattern aacr2DateMarkerPattern = Pattern.compile(".*\\b(ca\\.|fl\\.|cent[^u]|b\\.|d\\..*)");

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
			java.sql.Date moddate, String mainEntry, Map<String,Object> json) throws SQLException, IOException, XMLStreamException {
		MarcRecord oldR = null;
		int maxFieldId = 0;
		try (PreparedStatement getOldRecordStmt = authority.prepareStatement(
				"SELECT marc21 FROM authorityUpdate WHERE id = ? AND moddate < ? ORDER BY moddate DESC")){
			getOldRecordStmt.setString(1, id);
			getOldRecordStmt.setDate(2, moddate);
			try ( ResultSet rs = getOldRecordStmt.executeQuery() ) {
				while (rs.next()) {
					byte[] marc = rs.getBytes(1);
					try {
						MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY,marc);
						if (oldR == null) {
							oldR = r;
							maxFieldId = oldR.dataFields.last().id;
						} else {
							DataField f = getHeadField(r);
							f.id = ++maxFieldId ;
							oldR.dataFields.add(f);
						}
					} catch(IllegalArgumentException e) {
						e.printStackTrace();
					}
				}
			}
		}
		try (PreparedStatement getOldRecordStmt = authority.prepareStatement(
				"SELECT marcxml FROM voyagerAuthority WHERE id = ? AND moddate < ?")){
			getOldRecordStmt.setString(1, id);
			getOldRecordStmt.setDate(2, moddate);
			try ( ResultSet rs = getOldRecordStmt.executeQuery() ) {
				while (rs.next()) {
					String marc = rs.getString(1);
					try {
						MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY,marc,false);
						if (oldR == null) {
							oldR = r;
							maxFieldId = oldR.dataFields.last().id;
						} else {
							DataField f = getHeadField(r);
							f.id = ++maxFieldId ;
							oldR.dataFields.add(f);
						}
					} catch(IllegalArgumentException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return oldR;
	}

	private static void registerReportCompletion(
			Connection db, Map<String,String> authReportConfig, String firstFile, String lastFile,
			String toName, String toEmail, String file, List<String> boxIds)
			throws SQLException {
		if ( ! authReportConfig.containsKey("Mode") || ! authReportConfig.get("Mode").equalsIgnoreCase("test"))
			try (PreparedStatement stmt = db.prepareStatement(
							"UPDATE lcAuthorityFile SET reportedDate = NOW() WHERE updateFile BETWEEN ? AND ?")){
				stmt.setString(1, firstFile);
				stmt.setString(2, lastFile);
				stmt.executeUpdate();
			}
		String fullSubjectLine = String.format("%s: %s", authReportConfig.get("EmailSubject"), file);
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
				authReportConfig.get("EmailFrom"),
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

	private static String serializeActionable(Map<String, Change> actionableHeadings) {
		StringBuilder sb = new StringBuilder();
		for (String heading : actionableHeadings.keySet()) {
			sb.append(heading);
			EnumSet<DiffType> flags = actionableHeadings.get(heading).flags;
			if ( ! flags.isEmpty() )
				sb.append(": "+flags.toString());
			if ( actionableHeadings.get(heading).autoFlip != null)
				sb.append(" ("+actionableHeadings.get(heading).autoFlip.get("name")+")");
			sb.append("\n");
		}
		return sb.toString();
	}

	private static Map<String,Change> identifyActionableFields(
			MarcRecord r, boolean isDelete, String currentHeading) {
		Map<String,Change> fields = new HashMap<>();
		for (DataField f : r.dataFields) {
			if ( ! f.tag.startsWith("1") && ! f.tag.startsWith("4") && ! f.tag.startsWith("78")) continue;
			String heading = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
			String dashed_terms = f.concatenateSpecificSubfields(" > ", "vxyz");
			EnumSet<DiffType> flags = EnumSet.noneOf(DiffType.class);
			if (f.tag.startsWith("1") && ! isDelete) flags.add(DiffType.NEWMAIN);
			if (f.tag.startsWith("4") && heading.equals(currentHeading)) flags.add(DiffType.CURRENT);
			if ( heading.isEmpty() ) {
				if ( ! dashed_terms.isEmpty() ) fields.put(f.tag+" "+dashed_terms,new Change(flags));
			} else {
				if ( ! dashed_terms.isEmpty() ) heading += " > "+dashed_terms;
				fields.put(f.tag+" "+heading,new Change(flags));
			}
		}
		return fields;
	}

	private static Map<String,Change> identifyActionableChanges(
			Map<String,DataField> differences, MarcRecord r, DataField newMainF, String mainHeading, String currentHeading) {

		Map<String,Change> headings = new TreeMap<>();
		for ( String difference : differences.keySet()) {
			String tag = difference.substring(0, 3);
			DataField f = differences.get(difference);
			boolean isNew = f.mainTag.equals("NEW");
			if ( ! tag.startsWith("1") && ! tag.startsWith("4") && ! tag.startsWith("78")) continue;
			boolean newMain = tag.startsWith("1") && isNew;
			Map<String,EnumSet<DiffType>> diffHeadings = findHeadingVariants(f,newMain, mainHeading);
			for ( Entry<String,EnumSet<DiffType>> e : diffHeadings.entrySet() ) {
				e.getValue().add((isNew)?DiffType.NEW:DiffType.OLD);
				if ( ! isNew && f.tag.startsWith("1") && e.getValue().size() == 1) {
					Map<String,Object> autoFlip = lookForEligibleAutoFlip(newMainF, f);
					headings.putIfAbsent(e.getKey(),new Change(e.getValue(), autoFlip));
				}
				if (f.tag.startsWith("4") && e.getKey().equals(currentHeading))
					e.getValue().add(DiffType.CURRENT);
				headings.putIfAbsent(e.getKey(),new Change(e.getValue()));
			}
		}
		// pull in all the 4xx headings for updated records
		for ( DataField f : r.dataFields ) if ( f.tag.startsWith("4") ) {
			Map<String,EnumSet<DiffType>> diffHeadings = findHeadingVariants(f,false, mainHeading);
			for ( Entry<String,EnumSet<DiffType>> e : diffHeadings.entrySet() ) {
				e.getValue().add(DiffType.UNCH);
				headings.putIfAbsent(e.getKey(),new Change(e.getValue()));
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
		heading = heading.replaceAll(" ~~~$", "");
		heading = heading.replaceAll("[,\\\\]$", "");

		return new AbstractMap.SimpleEntry<>(heading,flags);
	}

	private static Map<String, EnumSet<DiffType>> findHeadingVariants(
			DataField f,boolean newMain, String preferredHeading) {

		String preferredClean = removeTrailingPunctuation(preferredHeading,"., ");

		Map<String,EnumSet<DiffType>> headings = new HashMap<>();
		String mainHead = f.concatenateSpecificSubfields("abcdefghjklmnopqrstu");
		Entry<String,EnumSet<DiffType>> mainEntry = headingOf(mainHead,f,null,newMain);
		if (mainEntry == null) return headings;
		headings.putIfAbsent(mainEntry.getKey(), mainEntry.getValue());

		if ( ! mainHead.isEmpty() ) {
			String mainHead_sansQ = f.concatenateSpecificSubfields("abcdefghjklmnoprstu");
			if ( ! mainHead_sansQ.equals(mainHead) && ! mainHead_sansQ.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansQ,f,DiffType.VAR_Q,false);
				if (entry != null) {
					String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
					if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
									extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
						headings.putIfAbsent(entry.getKey(), entry.getValue());
				}
			}

			String mainHead_sansD = f.concatenateSpecificSubfields("abcefghjklmnopqrstu");
			if ( ! mainHead_sansD.equals(mainHead) && ! mainHead_sansD.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansD,f,DiffType.VAR_D,false);
				if (entry != null) {
					String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
					if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
									extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
						headings.putIfAbsent(entry.getKey(), entry.getValue());
				}
			}

			String mainHead_sansQD = f.concatenateSpecificSubfields("abcefghjklmnoprstu");
			if ( ! mainHead_sansQD.equals(mainHead) && ! mainHead_sansQD.isEmpty() ) {
				Entry<String,EnumSet<DiffType>> entry = headingOf(mainHead_sansQD,f,DiffType.VAR_QD,false);
				if (entry != null) {
					String thisHeadingClean = removeTrailingPunctuation(entry.getKey(),"., ");
					if ( ! preferredClean.equals(thisHeadingClean) && ! headings.keySet().stream().anyMatch(
									extant -> thisHeadingClean.equals(removeTrailingPunctuation(extant,"., "))))
						headings.putIfAbsent(entry.getKey(), entry.getValue());
				}
			}
		}

		return headings;
	}
	private enum DiffType {
		NEWMAIN, DIACR, UNCH, VAR_Q, VAR_D, VAR_QD, OLD, NEW, CURRENT;
	}


	private static Map<String,Object> buildRelevantChange (
			String heading, String searchField, String blField, int records, EnumSet<DiffType> flags,
			Config config, Map<String,Object> autoFlip, boolean aspace, boolean looksLikeAACR2)
					throws UnsupportedEncodingException {

		Map<String,Object> rc = new HashMap<>();
		rc.put("heading", heading);
		String link;
		if ( ! aspace ) {
			if ( blField.endsWith("facet") )
				link = config.getBlacklightUrl()+"/?f["+blField+"][]="+heading;
			else
				link = config.getBlacklightUrl()+"/?q=%22"+
						URLEncoder.encode(heading, "UTF-8")+"%22&search_field="+blField;
			rc.put("blacklightLink", link);
			String solrLink = String.format("%s/select?qt=search&wt=csv&rows=9999999&fl=instance_id&q=%s:%%22%s%%22",
					config.getBlacklightSolrUrl(), blField, URLEncoder.encode(heading, "UTF-8"));
			rc.put("solrLink", solrLink);
		}

		String vocab = null;
		if      (blField.contains("_lc_"))    vocab = "lc";
		else if (blField.contains("_unk_"))   vocab = "unk";
		else if (blField.contains("_lcgft_")) vocab = "lcgft";
		else if (blField.contains("_lcjsh_")) vocab = "lcjsh";
		else if (blField.contains("_other_")) vocab = "other";
		else if (blField.contains("_fast_"))  vocab = "fast";
		if (vocab != null) rc.put("vocab", vocab);

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
		if (autoFlip != null && ! looksLikeAACR2
				&& (blField.contains("author") || "lc".equals(vocab) || "fast".equals(vocab)))
			rc.put("autoFlip", autoFlip.get("name"));

		return rc;
	}

	private static LinkedHashMap<String,DataField> serializeForComparison(MarcRecord marc) {
/*		marc.leader = "00000" + marc.leader.substring(5, 12) + "00000" + marc.leader.substring(17);
		ControlField five = null;
		for (ControlField f : marc.controlFields)
			if (f.tag.equals("005"))
				five = f;
			else if ( f.tag.equals("008") && f.value.length() > 6 )
				f.value = "000000"+f.value.substring(6);
		if (five != null)
			marc.controlFields.remove(five);*/
		LinkedHashMap<String,DataField> serializedFields = new LinkedHashMap<>();
		for ( DataField f : marc.dataFields ) {
			if ( f.tag.startsWith("1") ) f.ind2 = ' ';
			String fieldAsString = Normalizer.normalize(
					f.toString(), Normalizer.Form.NFC).replaceAll("\u0361(.)", "\uFE20$1\uFE21");
			if ( fieldAsString.startsWith("1") ) fieldAsString = removeTrailingPunctuation(fieldAsString,". ");
			serializedFields.put(fieldAsString, f);

		}
		return serializedFields;
	}
	private static Map<String,DataField> compareOldAndNewMarc(MarcRecord rec1, MarcRecord rec2) {
		Map<String,DataField> before = serializeForComparison(rec1);
		Map<String,DataField> after = serializeForComparison(rec2);
		List<String> common = new ArrayList<>();
		List<String> commonSortables = new ArrayList<>();
		for (String b : before.keySet())
			for (String a : after.keySet())
				if (b.equals(a)) {
					common.add(b);
				} else if (getFilingForm(b).equals(getFilingForm(a)))
					commonSortables.add(getFilingForm(b));
		Map<String, DataField> diff = new TreeMap<>();
		for (String b : before.keySet())
			if (!common.contains(b)) {
				if (! commonSortables.contains(getFilingForm(b))) {
					before.get(b).mainTag = "OLD";
					diff.put(b, before.get(b));
				} else {
					before.get(b).mainTag = "OLD";
					diff.put(b+" ~~~", before.get(b));
				}
			}
		for (String a : after.keySet())
			if (!common.contains(a))
				if (! commonSortables.contains(getFilingForm(a))) {
					after.get(a).mainTag = "NEW";
					diff.put(a, after.get(a));
				} else {
					after.get(a).mainTag = "NEW";
					diff.put(a+" ~~~", after.get(a));
				}
		return diff;
	}

	public static class Change {
		final EnumSet<DiffType> flags;
		final Map<String,Object> autoFlip;
		Change( EnumSet<DiffType> flags ) {
			this.flags = flags;
			this.autoFlip = null;
		}
		Change( EnumSet<DiffType> flags, Map<String,Object> autoFlip ) {
			this.flags = flags;
			this.autoFlip = autoFlip;
		}
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
