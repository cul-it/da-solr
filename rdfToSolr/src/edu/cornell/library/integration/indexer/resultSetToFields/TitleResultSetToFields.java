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
						String title_a = nodeToString(sol.get("value"));
						Integer offset = Integer.valueOf(nodeToString(sol.get("ind2")));
						SolrInputField title_t = new SolrInputField("title_t");
						title_t.addValue(title_a, 1);
						SolrInputField title_sort = new SolrInputField("title_sort");
						title_sort.addValue(title_a.substring(offset).toLowerCase(), 1);
						fields.put("title_sort", title_sort);
						if (sol.get("vern_val") != null) {
							String vern_val = nodeToString(sol.get("vern_val"));
							if (! vern_val.equals("")) {
								title_t.addValue(vern_val, 1);
								SolrInputField title_vern_display = new SolrInputField("title_vern_display");
								title_vern_display.addValue(vern_val, 1);
								fields.put("title_vern_display", title_vern_display);
							}
						}
						fields.put("title_t", title_t);
					} else if (nodeToString(sol.get("code")).equals("b")) {
						String title_b = nodeToString(sol.get("value"));
						SolrInputField subtitle_t = new SolrInputField("subtitle_t");
						subtitle_t.addValue(title_b, 1);
						if (sol.get("vern_val") != null) {
							String vern_val = nodeToString(sol.get("vern_val"));
							if (! vern_val.equals("")) {
								subtitle_t.addValue(vern_val, 1);
								SolrInputField subtitle_vern_display = new SolrInputField( "subtitle_vern_display");
								subtitle_vern_display.addValue(vern_val, 1);
								fields.put("subtitle_vern_display", subtitle_vern_display);									
							}							
						}
						fields.put("subtitle_t", subtitle_t);
					}
				}
			}
		}
		
		
		return fields;
	}


}
