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
		
		String vern_a = null;
		String vern_b = "";
		String title_a = null;
		String title_b = "";
		Integer ind2 = 0;
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			System.out.println(resultKey);
			if( rs != null){
				if (resultKey.equals("title_sort_offset")) {
					QuerySolution sol = rs.nextSolution();
					Integer offset = Integer.valueOf(nodeToString(sol.get("ind2")));
					if (offset > 0) 
						ind2 = offset;
				} else {
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						System.out.println( nodeToString(sol.get("code")) +  
								" => " + nodeToString(sol.get("value")));
	
						String code = nodeToString(sol.get("code"));
						String val = nodeToString(sol.get("value"));
						if (code.equals("a") ) {
							if (resultKey.equals("title_main")) {
								title_a = val;
							} else {
								vern_a = val;
							}
						} else if (code.equals("b")) {
							if (resultKey.equals("title_vern")) {
								title_b = val;
							} else {
								vern_b = val;
							}
						}
					}
				}
			}
		}
		addField(fields,"title_t",title_a);
		addField(fields,"title_t",vern_a);
		addField(fields,"title_vern_display",vern_a);
		addField(fields,"title_sort",title_a.substring(ind2).toLowerCase());
		addField(fields,"subtitle_t",title_b);
		if (! title_b.equals(vern_b))
			addField(fields,"subtitle_t",vern_b);
		addField(fields,"subtitle_vern_display",vern_b);		
		
		return fields;
	}


}
