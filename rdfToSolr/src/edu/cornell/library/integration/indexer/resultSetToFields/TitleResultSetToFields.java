package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class TitleResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();		
						
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					if (nodeToString(sol.get("code")).equals("a") ) {
						String title_a =nodeToString(sol.get("value"));
						addField(fields, "title_t", title_a);
												
						if (sol.get("vern_val") != null) {
							String vern_val = nodeToString(sol.get("vern_val"));
							if (! vern_val.equals("")) {
								addField(fields,"title_t",vern_val);
								addField(fields,"title_vern_display",vern_val);							
							}
						}						
					} else if (nodeToString(sol.get("code")).equals("b")) {
						String title_b = nodeToString(sol.get("value"));						
						addField(fields,"subtitle_t",title_b);						
						
						if (sol.get("vern_val") != null) {
							String vern_val = nodeToString(sol.get("vern_val"));
							if (! vern_val.equals("")) {
								
								addField(fields,"subtitle_t",vern_val);
								addField(fields,"subtitle_vern_display",vern_val);									
							}							
						}						
					}
				}
			}
		}
		
		
		return fields;
	}


}
