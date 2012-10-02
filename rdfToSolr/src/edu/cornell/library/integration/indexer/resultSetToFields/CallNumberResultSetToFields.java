package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class CallNumberResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		List<String> callnos = new ArrayList<String>();		
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			
			if ( resultKey.equals("holdings_callno")) {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String callno = nodeToString( sol.get("part1") );
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
		
		return fields;
	}


}
