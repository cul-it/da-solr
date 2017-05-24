package edu.cornell.library.integration.indexer.resultSetToFields;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class HathiLinks implements ResultSetToFields {

	protected static boolean debug = false;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<String> oclcids = new HashSet<>();
		Collection<String> barcodes = new HashSet<>();

		MarcRecord rec = new MarcRecord();
		for ( com.hp.hpl.jena.query.ResultSet rs : results.values() )
			rec.addDataFieldResultSet(rs);
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
		if (debug) {
			if (! oclcids.isEmpty() )
				for (String oclcid : oclcids)
					System.out.println("oclc: "+oclcid);
			if (! barcodes.isEmpty() )
				for (String barcode : barcodes)
					System.out.println("barcode: "+barcode);
		}

		Map<String,SolrInputField> fields = new HashMap<>();
		try (  Connection conn = config.getDatabaseConnection("Hathi")  )  {			
			SolrFieldValueSet vals = generateSolrFields(conn, /*oclcids,*/ barcodes);
			for (SolrField f : vals.fields)
				ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		}
		return fields;
	}

	public static SolrFieldValueSet generateSolrFields(Connection conn,
			/*Collection<String> oclcids,*/ Collection<String> barcodes ) throws SQLException, IOException {

		Map<String,Collection<String>> availableHathiMaterials = new HashMap<>();
		Collection<String> denyTitles = new HashSet<>();
		SolrFieldValueSet vals = new SolrFieldValueSet();

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
				vals.fields.addAll(URL.generateSolrFields(
						build856FieldSet("HathiTrust",
								"http://hdl.handle.net/2027/"+volumes.iterator().next())).fields);
			} else {
				// title link
				vals.fields.addAll(URL.generateSolrFields(
						build856FieldSet("HathiTrust (multiple volumes)",
								"http://catalog.hathitrust.org/Record/"+title)).fields);
			}
			vals.fields.add(new SolrField("hathi_title_data",title));
		}
		for ( String title : denyTitles ) {
			vals.fields.addAll(URL.generateSolrFields(
					build856FieldSet("HathiTrust – Access limited to full-text search",
							"http://catalog.hathitrust.org/Record/"+title)).fields);
			vals.fields.add(new SolrField("hathi_title_data",title));
		}
		if (availableHathiMaterials.size() > 0)
			vals.fields.add(new SolrField("online","Online"));

		if (debug)
			for (SolrField f : vals.fields)
				System.out.println( f.fieldName +": "+f.fieldValue);

		return vals;
	}

	private static DataFieldSet build856FieldSet( String description, String url) {
		DataField f = new DataField( 1, "856");
		f.subfields.add(new Subfield( 1, 'u', url));
		f.subfields.add(new Subfield( 2, 'z', description));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("856").addToFields(f).build();
		return fs;
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
	public static class SolrFieldValueSet {
		List<SolrField> fields = new ArrayList<>();
	}
}


/*
mysql> desc raw_hathi;
+------------------------------+--------------+------+-----+---------+-------+
| Field                        | Type         | Null | Key | Default | Extra |
+------------------------------+--------------+------+-----+---------+-------+
| Volume_Identifier            | varchar(128) | NO   | PRI |         |       |
| Access                       | varchar(16)  | YES  |     | NULL    |       |
| Rights                       | varchar(16)  | YES  |     | NULL    |       |
| UofM_Record_Number           | varchar(128) | YES  | MUL | NULL    |       |
| Enum_Chrono                  | varchar(128) | YES  | MUL | NULL    |       |
| Source                       | varchar(16)  | YES  | MUL | NULL    |       |
| Source_Inst_Record_Number    | varchar(128) | YES  |     | NULL    |       |
| OCLC_Numbers                 | varchar(250) | YES  | MUL | NULL    |       |
| ISBNs                        | varchar(250) | YES  |     | NULL    |       |
| ISSNs                        | varchar(250) | YES  |     | NULL    |       |
| LCCNs                        | varchar(250) | YES  |     | NULL    |       |
| Title                        | varchar(256) | YES  |     | NULL    |       |
| Imprint                      | varchar(256) | YES  |     | NULL    |       |
| Rights_determine_reason_code | varchar(8)   | YES  |     | NULL    |       |
| Date_Last_Update             | varchar(24)  | YES  |     | NULL    |       |
| Gov_Doc                      | int(1)       | YES  |     | NULL    |       |
| Pub_Date                     | varchar(16)  | YES  |     | NULL    |       |
| Pub_Place                    | varchar(128) | YES  |     | NULL    |       |
| Language                     | varchar(128) | YES  |     | NULL    |       |
| Bib_Format                   | varchar(16)  | YES  |     | NULL    |       |
| update_file_name             | varchar(128) | YES  |     | NULL    |       |
| record_counter               | int(12)      | YES  |     | NULL    |       |
+------------------------------+--------------+------+-----+---------+-------+
22 rows in set (0.00 sec)

 */
