package edu.cornell.library.integration.authority;


import static edu.cornell.library.integration.authority.IndexAuthorityRecords.getAllIdentifiers;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocumentList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class CompileRelationships {

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException {

		Collection<String> requiredArgs =  Config.getRequiredArgsForDB("Authority");
		requiredArgs.add("blacklightUrl");
		requiredArgs.add("blacklightSolrUrl");

		Config config = Config.loadConfig(requiredArgs);

		String outputFile = "report-"+Math.abs(UUID.randomUUID().toString().hashCode())+".tdf";
		System.out.println("Output file: "+outputFile);
		String blacklightUrl = config.getBlacklightUrl();

		try ( Connection authority = config.getDatabaseConnection("Authority") ;
				Http2SolrClient solr = new Http2SolrClient
						.Builder(config.getBlacklightSolrUrl())
						.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();
				BufferedWriter reportWriter = Files.newBufferedWriter(Paths.get(outputFile));
				){
			Set<String> identifiers = getAllIdentifiers(authority);
			List<String> headers = Arrays.asList("Identifier", "Main MARC Tag", "Main Heading", "Bib Count (1xx)",
					"Ref MARC Tag", "Ref Field","Bib Count (ref)", "$w", "$w relation", "$w instruction", "$i", "$4");
			reportWriter.append(String.join("\t", headers));
			reportWriter.append('\n');

			for (String identifier : identifiers) {
				String heading = null;
				MarcRecord rec = null;
				try ( PreparedStatement getAuthStmt = authority.prepareStatement("""
					SELECT marc21, heading, updateFile
					  FROM authorityUpdate
					 WHERE id = ?
					 ORDER BY moddate DESC
					 LIMIT 1""")) {
					getAuthStmt.setString(1, identifier);
					try (ResultSet rs = getAuthStmt.executeQuery()) {
						while (rs.next()) {
							rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getBytes("marc21"));
							heading = rs.getString("heading");
						}
					}
					
				}
				if (heading == null)
					try ( PreparedStatement getAuthStmt = authority.prepareStatement(
							"SELECT marcxml FROM voyagerAuthority WHERE id = ? ORDER BY moddate DESC LIMIT 1")) {
						getAuthStmt.setString(1, identifier);
						try (ResultSet rs = getAuthStmt.executeQuery()) {
							while (rs.next()) {
								rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getString("marcxml"),false);
								heading = mainHeading(rec);
							}
						} catch (IllegalArgumentException e) {
							System.out.format("ERROR: IllegalArgumentException %s (%s)\n", e.getMessage(), identifier);
						} catch (XMLStreamException e) {
							System.out.format("ERROR: XML Error %s (%s)\n",e.getMessage(),identifier);
						}
						
					}

				if (rec == null) continue;

				Character recordStatus = rec.leader.charAt(5);
				if ( recordStatus.equals('d') || recordStatus.equals('o')) continue;

				String mainTag = null;
				for (DataField f : rec.dataFields) if (f.tag.startsWith("1")) { mainTag = f.tag; break; }
				if ( mainTag == null ) continue;

				Map<String,Object> mhSearch = null;
				boolean mhSearched = false;
				Long mhFoundCount = null;

				// Look for 4xx or 5xx with $w or $i
				for (DataField f : rec.dataFields) {
					if ( ! f.tag.startsWith("4") && ! f.tag.startsWith("5") ) continue;
					String w = null;
					String i = null;
					String four = null;
					for (Subfield sf : f.subfields) {
						switch (sf.code) {
						case 'w':    w = sf.value; break;
						case 'i':    i = sf.value; break;
						case '4': four = sf.value; break;
						}
					}
					if (w == null && i == null) continue;
					List<String>  wLabel = getWDescription(w);
					if (! mhSearched ) {
						mhSearch = querySolrForMatchingBibCount(
								solr, getSolrSearchField(mainTag), heading, mainTag, blacklightUrl);
						mhSearched = true;
						mhFoundCount = (mhSearch == null) ?0L:(Long)mhSearch.get("bib count");
					}
					Map<String,Object> refSearch = querySolrForMatchingBibCount(
							solr, getSolrSearchField(f.tag), headingOf(f), f.tag, blacklightUrl);
					long refFoundCount = (refSearch == null)?0:(Long)refSearch.get("bib count");
					if ( mhSearch != null || refSearch != null) {
						System.out.format("%s (%s) %s : heading:%d, reference:%d\n",
								identifier, heading, f.toString(), mhFoundCount, refFoundCount);
					}
					List<String> tv = new ArrayList<>();
					tv.add(identifier);
					tv.add(mainTag);
					tv.add(heading);
					tv.add(String.valueOf(mhFoundCount));
					tv.add(f.tag);
					tv.add(f.toString());
					tv.add(String.valueOf(refFoundCount));
					tv.add((w == null)?"":w);
					if (wLabel == null) {
						tv.add("");
						tv.add("");
					} else {
						tv.add((wLabel.get(0) == null)?"":wLabel.get(0));
						tv.add((wLabel.get(1) == null)?"":wLabel.get(1));
					}
					tv.add((i==null)?"":i);
					tv.add((four==null)?"":four);
					for (int pos = 0; pos < tv.size(); pos++) tv.set(pos, tv.get(pos).replaceAll("\\s+", " "));
					reportWriter.append(String.join("\t", tv));
					reportWriter.append('\n');
				}

			}
			reportWriter.flush();
			reportWriter.close();

		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

	private static String getSolrSearchField(String fieldTag) {
		HeadingType ht = HeadingType.byAuthField("1"+fieldTag.substring(1, 3));
		switch (ht) {
		case PERS: return "subject_pers_browse";
		case CORP: return "subject_corp_browse";
		case MEETING:
		case EVENT:
			return "subject_event_browse";
		case WORK: return "subject_work_facet";
		case ERA: return "subject_era_facet";
		case TOPIC: return "subject_topic_facet";
		case PLACE: return "subject_geo_facet";
		case GENRE: return "subject_genr_facet";
		case SUB_GEN:
		case SUB_GEO:
		case SUB_ERA:
		case SUB_GNR:
			return "subject_sub_lc_browse";
		default: return null;
		}
	}

	private static List<String> getWDescription(String w) {
		String relationship = null;
		String instruction = null;
		if (w == null || w.length() == 0) return null;
		switch (w.charAt(0)) {
		case 'a': relationship = "Earlier heading"; break;
		case 'b': relationship = "Later heading"; break;
		case 'd': relationship = "Acronym"; break;
		case 'f': relationship = "Musical composition"; break;
		case 'g': relationship = "Broader term"; break;
		case 'h': relationship = "Narrower term"; break;
		case 'i': instruction = "<check subfield $i>"; break;
		case 'r': instruction = "<check subfield $i or $4>"; break;
		case 't': relationship = "Immediate parent body"; break;
		}
		if (w.length() >= 3 && w.charAt(2) =='a') relationship = "Earlier form of heading";
		if (w.length() >= 4) {
			switch (w.charAt(3)) {
			case 'a': instruction = "<don't display>"; break;
			case 'b': instruction = "<don't display - show 664 instead>"; break;
			case 'c': instruction = "<don't display - show 663 instead>"; break;
			case 'd': instruction = "<don't display - show 666 instead>"; break;
			}
		}
		return Arrays.asList(relationship, instruction);
	}

	private static String mainHeading( MarcRecord rec ) {
		for (DataField f : rec.dataFields) if (f.tag.startsWith("1")) {
			String main = f.concatenateSpecificSubfields("abcdefghijklmnopqrstu");
			String dashedTerms = f.concatenateSpecificSubfields(" > ", "vxyz");
			if ( ! main.isEmpty() && ! dashedTerms.isEmpty() )
				main += " > "+dashedTerms;
			return main;
		}
		return null;
	}

	private static String headingOf(DataField f ) {
		String main = f.concatenateSpecificSubfields("abcdefghijklmnopqrstu");
		String dashedTerms = f.concatenateSpecificSubfields(" > ", "vxyz");
		if ( ! main.isEmpty() && ! dashedTerms.isEmpty() )
			main += " > "+dashedTerms;
		return main;
	}

	public static Map<String,Object> querySolrForMatchingBibCount(
			Http2SolrClient solr,
			String field,
			String heading,
			String marcTag,
			String blacklightUrl
			)  throws SolrServerException, IOException {

		String query = field+":\""+heading.replaceAll("\"","'").replaceAll("\\\\","")+'"';
		SolrQuery q = new SolrQuery(query);
		q.setRows(0);
		q.setFields("instance_id","id");
		SolrDocumentList res = solr.query(q).getResults();
		long found = res.getNumFound();
		if (found == 0) return null;

		// build output block
		Map<String,Object> b = new HashMap<>();
		b.put("marc tag", marcTag);
		b.put("heading", heading);
		b.put("solr link", String.format("%s/select?qt=search&wt=csv&rows=99999&fl=instance_id&q=%s:%%22%s%%22",
				solr.getBaseURL(),field,URLEncoder.encode(heading, "UTF-8")));
		b.put("bib count", found);
		b.put("blacklight link",
				blacklightUrl+"/?q=%22"+URLEncoder.encode(heading, "UTF-8")+"%22&search_field="+field);
		return b;
	}

	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }

}
