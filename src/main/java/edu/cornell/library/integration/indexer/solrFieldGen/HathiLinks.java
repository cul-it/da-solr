package edu.cornell.library.integration.indexer.solrFieldGen;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class HathiLinks implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		for ( com.hp.hpl.jena.query.ResultSet rs : results.values() )
			JenaResultsToMarcRecord.addDataFieldResultSet(rec,rs);
		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("035","903"); }

	@Override
	// This field generator uses currently untracked HathiTrust content data, so should be regenerated more often.
	public Duration resultsShelfLife() { return Duration.ofDays(14); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {

		Collection<String> oclcids = new HashSet<>();
		Collection<String> barcodes = new HashSet<>();

		for (DataField f : rec.dataFields) {
			if (f.mainTag.equals("035")) {
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a'))
						if (sf.value.startsWith("(OCoLC"))
							oclcids.add(sf.value.substring(sf.value.lastIndexOf(')')+1));
			} else if (f.mainTag.equals("903")) {
				for (Subfield sf : f.subfields)
					if (sf.code.equals('p'))
						barcodes.add(sf.value);
			}
		}

		SolrFields sfs = new SolrFields();
		try (  Connection conn = config.getDatabaseConnection("Hathi")  )  {			
			Map<String,Collection<String>> availableHathiMaterials = new HashMap<>();
			Collection<String> denyTitles = new HashSet<>();

	/*		if (oclcids.size() > 0) {
	 			PreparedStatement pstmt = conn.prepareStatement
	 					("SELECT Volume_Identifier, UofM_Record_Number, Access FROM raw_hathi"
	 					+ " WHERE FIND_IN_SET( ? , OCLC_Numbers)");
	 			for (String oclcid : oclcids) {
					pstmt.setString(1, barcode);
					java.sql.ResultSet rs = pstmt.executeQuery();
					tabulateResults(rs);
				}
				pstmt.close();
			} */
			
			if (barcodes.size() > 0) {
				try (  PreparedStatement pstmt = conn.prepareStatement
						("SELECT Volume_Identifier, UofM_Record_Number, Access FROM raw_hathi"
						+ " WHERE Volume_Identifier = ?")  ) {
					for (String barcode : barcodes ) {
						pstmt.setString(1, "coo."+barcode);
						try ( java.sql.ResultSet rs = pstmt.executeQuery() ) {
							tabulateResults(rs,availableHathiMaterials,denyTitles); }
					}
				}
			}
			URL url = new URL();
			
			for ( String title : availableHathiMaterials.keySet() ) {
				Collection<String> volumes = availableHathiMaterials.get(title);
				int count = volumes.size();
				if (count == 1) {
					try ( PreparedStatement pstmt = conn.prepareStatement
							("SELECT COUNT(*) as count FROM raw_hathi"
							+ " WHERE UofM_Record_Number = ?")  ) {
						pstmt.setString(1, title);
						try (  java.sql.ResultSet rs = pstmt.executeQuery()  ) {
							while (rs.next())
								count = rs.getInt("count"); }
					}
				}
				if (count == 1) {
					// volume link
					sfs.addAll(url.generateSolrFields(buildMarcWith856("HathiTrust",
							"http://hdl.handle.net/2027/"+volumes.iterator().next()),null).fields);
				} else {
					// title link
					sfs.addAll(url.generateSolrFields(buildMarcWith856("HathiTrust (multiple volumes)",
							"http://catalog.hathitrust.org/Record/"+title),null).fields);
				}
				sfs.add(new SolrField("hathi_title_data",title));
			}
			for ( String title : denyTitles ) {
				sfs.addAll(url.generateSolrFields(buildMarcWith856(
						"HathiTrust – Access limited to full-text search",
						"http://catalog.hathitrust.org/Record/"+title),null).fields);
				sfs.add(new SolrField("hathi_title_data",title));
			}
		}
		return sfs;
	}

	private static MarcRecord buildMarcWith856( String description, String url) {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField( 1, "856");
		f.subfields.add(new Subfield( 1, 'u', url));
		f.subfields.add(new Subfield( 2, 'z', description));
		rec.dataFields.add(f);
		MarcRecord h = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		h.id = "1";
		h.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		rec.holdings.add(h);
		return rec;
	}
	private static void tabulateResults(java.sql.ResultSet rs,
			Map<String,Collection<String>> availableHathiMaterials,
			Collection<String> denyTitles) throws SQLException {
		while (rs.next()) {
			String vol = rs.getString("Volume_Identifier");
			String title = rs.getString("UofM_Record_Number");
			String access = rs.getString("Access");
			if (access.equals("allow")) {
				if ( ! availableHathiMaterials.containsKey(title) )
					availableHathiMaterials.put(title, new HashSet<String>() );
				availableHathiMaterials.get(title).add(vol);
			} else {
				denyTitles.add(title);
			}
		}
	}
}
