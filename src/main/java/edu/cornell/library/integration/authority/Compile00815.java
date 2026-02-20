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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocumentList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.generator.Subject.HeadingVocab;
import edu.cornell.library.integration.utilities.Config;

public class Compile00815 {

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException {

		Collection<String> requiredArgs =  Config.getRequiredArgsForDB("Authority");
		requiredArgs.add("blacklightUrl");
		requiredArgs.add("blacklightSolrUrl");

		Config config = Config.loadConfig(requiredArgs);

		String outputFile = "report-"+Math.abs(UUID.randomUUID().toString().hashCode())+".json";
		String blacklightUrl = config.getBlacklightUrl();

		try ( Connection authority = config.getDatabaseConnection("Authority") ;
				Http2SolrClient solr = new Http2SolrClient
						.Builder(config.getBlacklightSolrUrl())
						.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();
				BufferedWriter jsonWriter = Files.newBufferedWriter(Paths.get(outputFile));
				){
			Set<String> identifiers = getAllIdentifiers(authority);
			jsonWriter.append("[\n");
			boolean writtenJson = false;

			for (String identifier : identifiers) {
				String heading = null;
				MarcRecord rec = null;
				String recSource = null;
				try ( PreparedStatement getAuthStmt = authority.prepareStatement(
						"SELECT marc21, heading, updateFile FROM authorityUpdate WHERE id = ? ORDER BY moddate DESC LIMIT 1")) {
					getAuthStmt.setString(1, identifier);
					try (ResultSet rs = getAuthStmt.executeQuery()) {
						while (rs.next()) {
							rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getBytes("marc21"));
							heading = rs.getString("heading");
							recSource = rs.getString("updateFile");
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
								recSource = "voyager";
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

				Character eightFifteen = null;
				for (ControlField f : rec.controlFields) if (f.tag.equals("008")) eightFifteen = f.value.charAt(15);

				// 008 offset 15 = 'b' means heading is not appropriate for use as subject added entry
				if ( eightFifteen == null || ! eightFifteen.equals('b') ) continue;

				// Field 151 is a Geographic Name
				String mainTag = null;
				for (DataField f : rec.dataFields) if (f.tag.startsWith("1")) { mainTag = f.tag; break; }
				if ( mainTag == null || ! mainTag.equals("151")) continue;

				// Look for bibs matching main heading
				Map<String,Object> mhb = querySolrForMatchingBibCount(
						solr, "subject_geo_browse", heading, "151", blacklightUrl);
				if (mhb != null) {
					mhb.put("identifier", identifier); mhb.put("record source", recSource);
					if ( writtenJson ) jsonWriter.append(",\n"); else writtenJson = true;
					jsonWriter.append(mapper.writeValueAsString(mhb));
					jsonWriter.flush();
				}

				// Potentially look for bibs matching secondary subdivision heading
				String geographicSubdivision = get781Subdivision(rec);
				if (geographicSubdivision != null)
					for (HeadingVocab vocab : HeadingVocab.values()) {
						String solrField = "subject_sub_"+vocab.name().toLowerCase()+"_browse";
						Map<String,Object> sdb = querySolrForMatchingBibCount(
								solr, solrField, geographicSubdivision, "781", blacklightUrl);
						if (sdb != null) {
							sdb.put("identifier", identifier); sdb.put("record source", recSource);
							if ( writtenJson ) jsonWriter.append(",\n"); else writtenJson = true;
							jsonWriter.append(mapper.writeValueAsString(sdb));
						}

					}

			}
			jsonWriter.append("]\n");
			jsonWriter.flush();
			jsonWriter.close();

		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

	private static String get781Subdivision(MarcRecord rec) {
		String subdivision = null;
		for (DataField f : rec.dataFields) if (f.tag.equals("781"))
			subdivision = f.concatenateSpecificSubfields(" > ", "z");
		return subdivision;
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
		b.put("solr link",
				solr.getBaseURL()+"/select?qt=search&wt=csv&rows=99999&fl=instance_id&q="+query.replaceAll("\"", "%22"));
		b.put("bib count", found);
		if (marcTag.equals("151"))
			b.put("blacklight link",
					blacklightUrl+"/?q=%22"+URLEncoder.encode(heading, "UTF-8")+"%22&search_field="+field);
		return b;
	}

	static ObjectMapper mapper = new ObjectMapper();
	static { mapper.enable(SerializationFeature.INDENT_OUTPUT); }

}
