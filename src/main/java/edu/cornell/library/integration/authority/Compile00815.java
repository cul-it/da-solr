package edu.cornell.library.integration.authority;


import static edu.cornell.library.integration.authority.IndexAuthorityRecords.getAllIdentifiers;
import static edu.cornell.library.integration.authority.Solr.querySolrForMatchingBibCount;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.generator.Subject.HeadingVocab;
import edu.cornell.library.integration.utilities.Config;

public class Compile00815 {

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException {

		Collection<String> requiredArgs =  Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		System.out.println("identifier\trecord source\tMARC field\theading\tbib count");

		try ( Connection authority = config.getDatabaseConnection("Authority") ;
				Http2SolrClient solr = new Http2SolrClient
						.Builder(config.getBlacklightSolrUrl())
						.withBasicAuthCredentials(config.getSolrUser(),config.getSolrPassword()).build();
				){
			Set<String> identifiers = getAllIdentifiers(authority);

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
				int headingMatches = querySolrForMatchingBibCount(solr, "subject_geo_browse", heading, false);
				if (headingMatches > 0)
					System.out.format("%s\t%s\t151\t%s\t%d\n", identifier, recSource, heading, headingMatches);


				String geographicSubdivision = get781Subdivision(rec);
				if (geographicSubdivision != null) {

					headingMatches = querySolrForMatchinSubdivisionBibCount(solr, geographicSubdivision);
					if (headingMatches > 0)
						System.out.format("%s\t%s\t781\t%s\t%d\n", identifier, recSource, geographicSubdivision, headingMatches);
				}

			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		}
	}

	private static int querySolrForMatchinSubdivisionBibCount(Http2SolrClient solr, String heading) throws SolrServerException, IOException {
		int count = 0;
		for (HeadingVocab vocab : HeadingVocab.values()) {
			String solrField = "subject_sub_"+vocab.name().toLowerCase()+"_browse";
			count += querySolrForMatchingBibCount(solr, solrField, heading, false);
		}
		return count;
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

}
