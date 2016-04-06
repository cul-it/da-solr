package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * use OCLC Work IDs to map OCLC IDs against one another to identify matching titles
 * 
 */
public class TitleMatchRSTF implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();
		int bibid = 0;
		
		com.hp.hpl.jena.query.ResultSet rs = results.get("bib_id");
		if( rs != null){
			while(rs.hasNext()){
				QuerySolution sol = rs.nextSolution();
				bibid = Integer.valueOf(nodeToString( sol.get("id")));
			}
		}
		if (bibid == 0) return solrFields;
		
		Connection conn = null;
		try {
			conn = config.getDatabaseConnection("Workids");
			PreparedStatement pstmt = conn.prepareStatement
					("   SELECT workRecCount.* FROM voy2work" +
					" LEFT JOIN workRecCount on (voy2work.workid = workRecCount.workid) "
					+ "   WHERE bibid = ?");
			pstmt.setInt(1, bibid);
			java.sql.ResultSet workResult = pstmt.executeQuery();
			int workid = 0, voyCount = 0;
			while (workResult.next()) {
				workid = workResult.getInt("workid");
				voyCount = workResult.getInt("voyCount");
			}
			pstmt.close();
			addField(solrFields,"workid_facet",String.valueOf(workid));
			addField(solrFields,"workid_display",String.valueOf(workid)+"|"+String.valueOf(voyCount));
			if (workid == 0 || voyCount == 1) return solrFields;
			
			PreparedStatement pstmt2 = conn.prepareStatement
					("SELECT voy2loc.* " +
					   "FROM voy2work LEFT JOIN voy2loc ON (voy2loc.bibid = voy2work.bibid) " +
					  "WHERE workid = ?");
			pstmt2.setInt(1, workid);
			java.sql.ResultSet bibResult = pstmt2.executeQuery();
			List<BibRec> otherBibs = new ArrayList<BibRec>();
			String mainRecFormat = null;
			while (bibResult.next()) {
				int otherBib = bibResult.getInt("bibid");
				if (otherBib == bibid) {
					mainRecFormat = bibResult.getString("format");
					continue;
				}
				String format = bibResult.getString("format");
				String online = bibResult.getString("online");
				String library = bibResult.getString("library");
				if (online != null)
					if (library != null)
						otherBibs.add(new BibRec(otherBib,format,"Online/At the Library"));
					else
						otherBibs.add(new BibRec(otherBib,format,online));
				else
					if (library != null)
						otherBibs.add(new BibRec(otherBib,format,library));
					else
						otherBibs.add(new BibRec(otherBib,format,"unavailable"));
			}
			for (int i = 0; i < otherBibs.size(); i++) {
				BibRec bib = otherBibs.get(i);
				if (bib.format.equals(mainRecFormat))
					addField(solrFields,"other_availability_piped",String.valueOf(bib.bibid)+"|"+bib.location);
				else
					addField(solrFields,"other_availability_piped"
							,String.valueOf(bib.bibid)+"|"+bib.format+": "+bib.location);
			}
			
		} finally {
			if (conn != null)
				conn.close();
		}
		
		
/*		Collection<String> allIds = new HashSet<String>();
		Collection<String> matchDescs = new HashSet<String>();
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					String id = nodeToString( sol.get("id"));
					if (id.equals("6308404")) {
						allIds.add(id);
						allIds.add("8377804");
						matchDescs.add("8377804||Online");
					} else if (id.equals("8377804")) {
						allIds.add(id);
						allIds.add("6308404");
						matchDescs.add("6308404||Print");
						
						
					} else if (id.equals("4183296")) {
						allIds.add("4183296");
						allIds.add("5310953");
						allIds.add("5194294");
						matchDescs.add("5194294||Online");
//						matchDescs.add("5194294|London : printed by Thomas Davison; for James Ridgway, [1790] "
//								+ "A new edition corrected and enlarged.|Online");
						matchDescs.add("5310953|printed for J. Ridgway, 1790. "
								+ "The third edition.|Online");
					} else if (id.equals("5310953")) {
						allIds.add("4183296");
						allIds.add("5310953");
						allIds.add("5194294");
						matchDescs.add("4183296|printed by Thomas Davison; for James Ridgway, [1790] "
						+ "A new edition corrected and enlarged.|Microform");
						matchDescs.add("5194294|printed by Thomas Davison; for James Ridgway, [1790] "
						+ "A new edition corrected and enlarged.|Online");
					} else if (id.equals("5194294")) {
						allIds.add("4183296");
						allIds.add("5310953");
						allIds.add("5194294");
						matchDescs.add("4183296||Microform");
//						matchDescs.add("5194294|London : printed by Thomas Davison; for James Ridgway, [1790] "
//								+ "A new edition corrected and enlarged.|Online");
						matchDescs.add("5310953|printed for J. Ridgway, 1790. "
								+ "The third edition.|Online");
						
						
					} else if (id.equals("1267803")) {
						allIds.add("1267803"); //Firenze : L. Ciardetti, 1826.
						allIds.add("1263497");
						matchDescs.add("1263497|Bologna : Tip. Cardinali e Frulli, 1826-27.|Print");
						allIds.add("1281328");
						matchDescs.add("1281328|Bassano : Remondini, 1826.|Print");
						allIds.add("1281361");
						matchDescs.add("1281361|Londra : J. Murray, 1826-27.|Print");
						allIds.add("1355779");
						matchDescs.add("1355779|Bologna : Tipi Gamberini Parmeggiani, 1826.|Print");
					} else if (id.equals("1263497")) {
						allIds.add("1263497");
//						matchDescs.add("1263497|Bologna : Tip. Cardinali e Frulli, 1826-27.");
						allIds.add("1267803");
						matchDescs.add("1267803|Firenze : L. Ciardetti, 1826.|Print");
						allIds.add("1281328");
						matchDescs.add("1281328|Bassano : Remondini, 1826.|Print");
						allIds.add("1281361");
						matchDescs.add("1281361|Londra : J. Murray, 1826-27.|Print");
						allIds.add("1355779");
						matchDescs.add("1355779|Bologna : Tipi Gamberini Parmeggiani, 1826.|Print");
					} else if (id.equals("1281328")) {
						allIds.add("1281328");
//						matchDescs.add("1281328|Bassano : Remondini, 1826.");
						allIds.add("1263497");
						matchDescs.add("1263497|Bologna : Tip. Cardinali e Frulli, 1826-27.|Print");
						allIds.add("1267803");
						matchDescs.add("1267803|Firenze : L. Ciardetti, 1826.|Print");
						allIds.add("1281361");
						matchDescs.add("1281361|Londra : J. Murray, 1826-27.|Print");
						allIds.add("1355779");
						matchDescs.add("1355779|Bologna : Tipi Gamberini Parmeggiani, 1826.|Print");
					} else if (id.equals("1281361")) {
						allIds.add("1281361");
//						matchDescs.add("1281361|Londra : J. Murray, 1826-27.");
						allIds.add("1281328");
						matchDescs.add("1281328|Bassano : Remondini, 1826.|Print");
						allIds.add("1263497");
						matchDescs.add("1263497|Bologna : Tip. Cardinali e Frulli, 1826-27.|Print");
						allIds.add("1267803");
						matchDescs.add("1267803|Firenze : L. Ciardetti, 1826.|Print");
						allIds.add("1355779");
						matchDescs.add("1355779|Bologna : Tipi Gamberini Parmeggiani, 1826.|Print");
					} else if (id.equals("1355779")) {
						allIds.add("1355779");
//						matchDescs.add("1355779|Bologna : Tipi Gamberini Parmeggiani, 1826.");
						allIds.add("1281328");
						matchDescs.add("1281328|Bassano : Remondini, 1826.|Print");
						allIds.add("1263497");
						matchDescs.add("1263497|Bologna : Tip. Cardinali e Frulli, 1826-27.|Print");
						allIds.add("1267803");
						matchDescs.add("1267803|Firenze : L. Ciardetti, 1826.|Print");
						allIds.add("1281361");
						matchDescs.add("1281361|Londra : J. Murray, 1826-27.|Print");

						
						
					} else if (id.equals("4370947")) {
						allIds.add("4370947");
//						matchDescs.add("4370947|Boston : Addison-Wesley, c2003. 3rd ed.");
						allIds.add("7186490");
						matchDescs.add("7186490|c2012. 4th ed.|Print");
						allIds.add("3481747");
						matchDescs.add("3481747|Reading, Mass. : c2000. 2nd ed. update.|Print");
						allIds.add("2906072");
						matchDescs.add("2906072|Reading, Mass. : 1996. 2nd ed., Instructors ed.|Print");
						allIds.add("2889921");
						matchDescs.add("2889921|Reading, Mass. : c1994. Instructor's ed.|Print");
					} else if (id.equals("2889921")) {
						allIds.add("2889921");
//						matchDescs.add("2889921|Reading, Mass. : Addison-Wesley, c1994. Instructor's ed.");
						allIds.add("7186490");
						matchDescs.add("7186490|Boston : c2012. 4th ed.|Print");
						allIds.add("4370947");
						matchDescs.add("4370947|Boston : c2003. 3rd ed.|Print");
						allIds.add("3481747");
						matchDescs.add("3481747|c2000. 2nd ed. update.|Print");
						allIds.add("2906072");
						matchDescs.add("2906072|1996. 2nd ed., Instructors ed.|Print");
					} else if (id.equals("3481747")) {
						allIds.add("3481747");
//						matchDescs.add("3481747|Reading, Mass. : Addison-Wesley, c2000. 2nd ed. update.");
						allIds.add("7186490");
						matchDescs.add("7186490|Boston : c2012. 4th ed.|Print");
						allIds.add("4370947");
						matchDescs.add("4370947|Boston : c2003. 3rd ed.|Print");
						allIds.add("2906072");
						matchDescs.add("2906072|1996. 2nd ed., Instructors ed.|Print");
						allIds.add("2889921");
						matchDescs.add("2889921|c1994. Instructor's ed.|Print");
					} else if (id.equals("2906072")) {
						allIds.add("2906072");
//						matchDescs.add("2906072|Reading, Mass. : Addison-Wesley, 1996. 2nd ed., Instructors ed.");
						allIds.add("7186490");
						matchDescs.add("7186490|Boston : c2012. 4th ed.|Print");
						allIds.add("4370947");
						matchDescs.add("4370947|Boston : c2003. 3rd ed.|Print");
						allIds.add("3481747");
						matchDescs.add("3481747|c2000. 2nd ed. update.|Print");
						allIds.add("2889921");
						matchDescs.add("2889921|c1994. Instructor's ed.|Print");
					} else if (id.equals("7186490")) {
						allIds.add("7186490");
//						matchDescs.add("7186490|Boston : Addison-Wesley, c2012. 4th ed.");
						allIds.add("4370947");
						matchDescs.add("4370947|c2003. 3rd ed.|Print");
						allIds.add("3481747");
						matchDescs.add("3481747|Reading, Mass. : c2000. 2nd ed. update.|Print");
						allIds.add("2906072");
						matchDescs.add("2906072|Reading, Mass. : 1996. 2nd ed., Instructors ed.|Print");
						allIds.add("2889921");
						matchDescs.add("2889921|Reading, Mass. : c1994. Instructor's ed.|Print");
						
						
					} else if (id.equals("8128115")) {
						allIds.add("8128115");
						allIds.add("8185681");
						matchDescs.add("8185681||Online");
					} else if (id.equals("8185681")) {
						allIds.add("8185681");
						allIds.add("8128115");
						matchDescs.add("8128115||Online");
					}
				}		
			}
		} 
							
		for (String s: allIds) {
				addField(solrFields,"title_match_t",s);	
		}
		for (String s: matchDescs) {
			addField(solrFields,"title_match_piped",s);
		}*/
		return solrFields;	
	}
	
	private class BibRec {
		int bibid;
		String format;
		String location;
		
		public BibRec(int id, String form, String loc) {
			bibid = id;
			format = form;
			location = loc;
		}
	}
}
