package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class CallNumberResultSetToFields implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> callnos = new HashSet<String>();
		Collection<String> letters = new HashSet<String>();
		
		this.getClass().getResourceAsStream("callnumber_map.properties");
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			
			if ( resultKey.equals("letter_subject")) {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String letter = nodeToString( sol.get("code") );
						String subject = nodeToString( sol.get("subject") );
						addField(fields,"lc_1letter_facet",letter+" - "+subject);
					}
				}
			} else {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String callno = nodeToString( sol.get("part1") );
						letters.add( callno.substring(0,1) );
						if (sol.contains("part2")) {
							String part2 = nodeToString( sol.get("part2") );
							if (! part2.equals("")) {
								callno += " " + part2;
							}
						}
						callnos.add(callno);
					}
				}
			}
		}

		Iterator<String> i = callnos.iterator();
		while (i.hasNext()) {
			String callno = i.next();
			addField(fields,"lc_callnum_display",callno);
		}		
		
		i = letters.iterator();
		while (i.hasNext()) {
			String l = i.next();
			System.out.println(l);
			step.addMainStoreQuery("letter_subject",
			    		"SELECT ?code ?subject\n" +
			    		"WHERE {\n" +
			    		" ?lc intlayer:code \""+l+"\".\n" +
			    		" ?lc intlayer:code ?code.\n" +
			    		" ?lc rdfs:label ?subject. }\n" );
//			temp.buildFields(recordURI, mainStore, localStore);
		}
		
		
		step.setFields(fields);
		return step;
	}


}
