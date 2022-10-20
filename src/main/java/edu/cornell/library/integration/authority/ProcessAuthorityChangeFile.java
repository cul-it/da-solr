package edu.cornell.library.integration.authority;

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
import java.text.Normalizer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.cornell.library.integration.utilities.Config;

public class ProcessAuthorityChangeFile {

	public static void main(String[] args) throws IOException, SQLException, SolrServerException {

		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(        Config.getRequiredArgsForDB("Authority") );
		requiredArgs.add("blacklightUrl");
		requiredArgs.add("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredArgs);

		String firstFile = "unname21.08";
		String lastFile =  "unname21.08";

		try ( Connection authority = config.getDatabaseConnection("Authority");
				PreparedStatement getOldRecordStmt = authority.prepareStatement(
						"SELECT marc21 FROM voyagerAuthority WHERE id = ?");
				HttpSolrClient solr = new HttpSolrClient(config.getBlacklightSolrUrl());
				PreparedStatement getAuthStmt = authority.prepareStatement(
						"SELECT marc21, changeType, updateFile, id, heading, undifferentiated, vocabulary"+
						"  FROM authorityUpdate"+
						" WHERE updateFile BETWEEN ? AND ?"+
						" ORDER BY updateFile, positionInFile");
				BufferedWriter jsonWriter = Files.newBufferedWriter(Paths.get(firstFile+".json"))) {
//			BufferedWriter jsonWriter = Files.newBufferedWriter(
//					Paths.get(firstFile+"-"+lastFile.substring(lastFile.length()-2)+".json"))) {

			
			jsonWriter.append("[\n");
			boolean writtenJson = false;

			Set<String> entailedBibs = new HashSet<>();
			int count = 0;
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

					if ( records.getBoolean("undifferentiated") )
						json.put("undifferentiated", true);

					MarcRecord oldR = null;
					if ( lookForOldRecordVersion ) {
						getOldRecordStmt.setString(1, id);
						try ( ResultSet rs = getOldRecordStmt.executeQuery() ) {
							while (rs.next()) {
								byte[] marc = rs.getBytes(1);
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
								json.put("oldHeading", oldMainEntry);
							}
						} catch ( IllegalArgumentException e) {
							continue;
						}
					}

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
						String fieldType = field.substring(1, 3);
						String heading = field.substring(4);
						if ( checkedHeadings.contains(heading) ) continue;
						checkedHeadings.add(heading);
						if ( mainEntrySort.equals(getFilingForm(heading)))
							flags.add(DiffType.DIACR);

						List<String> searchFields = identifySearchFields(fieldType,vocab);
						if ( searchFields == null ) continue;

						for (String searchField : searchFields) {
							if ( flags.contains(DiffType.NEWMAIN)
									&& changeType.equals(ChangeType.UPDATE)
									&& ! ( searchField.contains("_unk_") ) ) continue;
							SolrQuery q = new SolrQuery(searchField+":\""+heading.replaceAll("\"","'")+'"');
							q.setRows(1_000);
							q.setFields("instance_id","id");

							SolrDocumentList res = solr.query(q).getResults();
							Long recordCount = res.getNumFound();
							if ( recordCount == 0 ) continue;
							Set<String> recordSet = new HashSet<>();
							for (SolrDocument doc : res) recordSet.add((String)doc.getFieldValue("id"));
							entailedBibs.addAll(recordSet);
							if ( flags.contains(DiffType.DIACR) ) {
								String facetField = (searchField.startsWith("author")
										?"author_facet":searchField.replace("browse","facet"));
								Map<String,Long> displayForms = tabulateActualUnnormalizedHeadings(
										solr, heading, searchField, facetField);
								for ( String displayForm : displayForms.keySet() ) {
									if (displayForm.equals(mainEntry)
											&& ! searchField.contains("_unk_")) continue;
									System.out.printf("%s relevant w/ %d instances (%s).\n",
											displayForm,displayForms.get(displayForm),searchField);
									relevantChanges.add(buildRelevantChange(displayForm,searchField,facetField,
											displayForms.get(displayForm), flags, config));
								}
							} else {
								relevantChanges.add(buildRelevantChange(
										heading,searchField,searchField,recordCount, flags, config));
							}
						}
					}
					if ( ! relevantChanges.isEmpty() ) {
						json.put("relevantChanges", relevantChanges);
						ObjectMapper mapper = new ObjectMapper();
						mapper.enable(SerializationFeature.INDENT_OUTPUT);
						if ( writtenJson )
							jsonWriter.append(",\n");
						else
							writtenJson = true;
						jsonWriter.append(mapper.writeValueAsString(json));
					}
					count++;

				}
				jsonWriter.append("]");
				jsonWriter.flush();
				jsonWriter.close();
				System.out.println(count);
				System.out.println("Entailed bibs:");
				for (String entailedBib : entailedBibs)
					System.out.println(entailedBib);
			}
		}
	}
//	static Pattern dateMatcher = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2}).*");

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

	private static List<String> identifySearchFields(String fieldType, AuthoritySource vocab) {
		List<String> searchFields = new ArrayList<>();
		switch (fieldType) {
		case "00":
			searchFields.add("author_pers_roman_browse");
			searchFields.add("subject_pers_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_pers_lcjsh_browse");
			else searchFields.add("subject_pers_lc_browse");
			break;
		case "10":
			searchFields.add("author_corp_roman_browse");
			searchFields.add("subject_corp_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_corp_lcjsh_browse");
			else searchFields.add("subject_corp_lc_browse");
			break;
		case "11":
			searchFields.add("author_event_roman_browse");
			searchFields.add("subject_event_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_event_lcjsh_browse");
			else searchFields.add("subject_event_lc_browse");
			break;
		case "30":
			searchFields.add("title_exact");
			searchFields.add("subject_work_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_work_lcjsh_browse");
			else searchFields.add("subject_work_lc_browse");
			break;
		case "48":
			searchFields.add("subject_era_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_era_lcjsh_browse");
			else searchFields.add("subject_era_lc_browse");
			break;
		case "50":
			searchFields.add("subject_topic_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_topic_lcjsh_browse");
			else searchFields.add("subject_topic_lc_browse");
			break;
		case "51":
			searchFields.add("subject_geo_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_geo_lcjsh_browse");
			else searchFields.add("subject_geo_lc_browse");
			break;
		case "55":
			searchFields.add("subject_genr_lcgft_browse");
			searchFields.add("subject_genr_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_genr_lcjsh_browse");
			else searchFields.add("subject_genr_lc_browse");
			break;
		case "62":
			searchFields = Arrays.asList("notes_t");
			break;
		case "80":
		case "81":
		case "82":
		case "85":
			searchFields.add("subject_sub_unk_browse");
			if ( vocab.equals(AuthoritySource.LCJSH) )
				searchFields.add("subject_sub_lcjsh_browse");
			else searchFields.add("subject_sub_lc_browse");
			break;
		default:
			System.out.println("Unexpected field type "+fieldType);
			return null;
		}
		return searchFields;
	}

	private static Map<String,Object> buildRelevantChange (
			String heading, String searchField, String blField, Long records, EnumSet<DiffType> flags,
			Config config)
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
			return null;
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
