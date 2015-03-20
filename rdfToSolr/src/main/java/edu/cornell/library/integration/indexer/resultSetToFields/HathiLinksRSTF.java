package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class HathiLinksRSTF implements ResultSetToFields {

	protected boolean debug = true;
	Map<String,Collection<String>> availableHathiMaterials = new HashMap<String,Collection<String>>();
	Collection<String> denyTitles = new HashSet<String>();

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> oclcids = new HashSet<String>();
		Collection<String> barcodes = new HashSet<String>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			if (debug) System.out.println("Result Key: "+resultKey);
			if( rs != null)
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if (debug) System.out.println("Field: "+name);
						if (name.equals("thirtyfive")) {
							String val = nodeToString( node );
							if (val.trim().startsWith("(OCoLC)")) {
								String oclcid = val.substring(val.lastIndexOf(')')+1);
								oclcids.add(oclcid.trim());
								if (debug) System.out.println("oclcid = "+oclcid);
							}
						} else if (name.equals("barcode")) {
							barcodes.add(nodeToString( node ));
							if (debug) System.out.println("barcode = "+nodeToString( node ));
						}

					}
				}
		}
		Connection conn = config.getDatabaseConnection("Hathi");
		
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
			PreparedStatement pstmt = conn.prepareStatement
					("SELECT Volume_Identifier, UofM_Record_Number, Access FROM raw_hathi"
					+ " WHERE Volume_Identifier = ?");
			for (String barcode : barcodes ) {
				pstmt.setString(1, "coo."+barcode);
				java.sql.ResultSet rs = pstmt.executeQuery();
				tabulateResults(rs);
			}
			pstmt.close();
		}
		
		for ( String title : availableHathiMaterials.keySet() ) {
			Collection<String> volumes = availableHathiMaterials.get(title);
			int count = volumes.size();
			if (count == 1) {
				PreparedStatement pstmt = conn.prepareStatement
						("SELECT COUNT(*) as count FROM raw_hathi"
						+ " WHERE UofM_Record_Number = ?");
				pstmt.setString(1, title);
				java.sql.ResultSet rs = pstmt.executeQuery();
				while (rs.next())
					count = rs.getInt("count");
				pstmt.close();
			}
			if (count == 1) {
				// volume link
				addField(fields,"url_access_display",
						"http://hdl.handle.net/2027/"+volumes.iterator().next()+"|HathiTrust");
			} else {
				// title link
				addField(fields,"url_access_display",
						"http://catalog.hathitrust.org/Record/"+title+"|HathiTrust (multiple volumes)");
			}
			addField(fields,"hathi_title_data",title);
		}
		for ( String title : denyTitles ) {
			addField(fields,"url_other_display",
					"http://catalog.hathitrust.org/Record/"+title+"|HathiTrust – Access limited to full-text search");
			addField(fields,"hathi_title_data",title);
		}
		if (availableHathiMaterials.size() > 0)
			addField(fields,"online","Online");
		
		if (debug)
			for (SolrInputField f : fields.values())
				System.out.println( f.getName() +": "+StringUtils.join(f.getValues(), ", "));
		
		return fields;
	}

	private void tabulateResults(java.sql.ResultSet rs) throws SQLException {
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
