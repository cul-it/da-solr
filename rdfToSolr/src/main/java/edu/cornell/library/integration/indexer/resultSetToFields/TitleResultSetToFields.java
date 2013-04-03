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
		
		String title_a = null;
		String title_b = "";
		Integer ind2 = 0;
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				if (resultKey.equals("title_sort_offset")) {
					if( rs.hasNext() ){
						QuerySolution sol = rs.nextSolution();
						String ind2string = nodeToString(sol.get("ind2"));
						if (Character.isDigit(ind2string.charAt(0))) {
							Integer offset = Integer.valueOf(ind2string);
							if (offset > 0) 
								ind2 = offset;
						}
					}/*TODO: should problems here be ignored or are they errors?
					  else{
						throw new Exception("Expected title_sort_offset in ind2 but there were no results");
					}*/
				} else {
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String code = nodeToString(sol.get("code"));
						String val = nodeToString(sol.get("value"));
						if (val == null) continue;
						if (code.equals("a") ) {
							title_a = val;
						} else if (code.equals("b")) {
							title_b = val;
						}
					}
				}
			}
		}
		
		if( title_a != null && title_b != null && title_a.substring(ind2) != null ){
			addField(fields,"title_sort",RemoveTrailingPunctuation(title_a.substring(ind2).toLowerCase(),":/ ")+
					" " + title_b.toLowerCase());
		}
		
		return fields;
	}


}
