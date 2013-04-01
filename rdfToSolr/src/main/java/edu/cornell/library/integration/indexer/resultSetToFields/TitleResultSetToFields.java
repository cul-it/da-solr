package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;
import org.mortbay.log.Log;

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
		String vern_n = "";
		String vern_p = "";
		String title_a = null;
		String title_b = "";
		String title_n = "";
		String title_p = "";
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
							if (resultKey.equals("title_main")) {
								title_a = val;
							} else {
								vern_a = val;
							}
						} else if (code.equals("b")) {
							if (resultKey.equals("title_main")) {
								title_b = val;
							} else {
								vern_b = val;
							}
					
						} else if (code.equals("n")) {
							if (resultKey.equals("title_main")) {
								title_n = val;
							} else {
								vern_n = val;
							}
					
						} else if (code.equals("p")) {
							if (resultKey.equals("title_main")) {
								title_p = val;
							} else {
								vern_p = val;
							}
						}
					}
				}
			}
		}
		addField(fields,"title_t",title_a + " " + title_n + " " + title_p + " " + vern_a + " " + vern_n + " " + vern_p);
		addField(fields,"title_vern_display",vern_a);
		
		if( title_a != null && title_b != null && title_a.substring(ind2) != null ){
			addField(fields,"title_sort",RemoveTrailingPunctuation(title_a.substring(ind2).toLowerCase(),":/ ")+
					" " + title_b.toLowerCase());
		}
		
		addField(fields,"subtitle_t",RemoveTrailingPunctuation(title_b,":/ "));
		if (! title_b.equals(vern_b)) {
			addField(fields,"subtitle_t",vern_b);
			addField(fields,"subtitle_vern_display",RemoveTrailingPunctuation(vern_b,":/ "));
		}
		String title_display = title_a;
		if (! title_n.equals("")) title_display += " " + title_n;
		if (! title_p.equals("")) title_display += " " + title_p;
		addField(fields,"title_display",RemoveTrailingPunctuation(title_display,":/ "));
		addField(fields,"subtitle_display",RemoveTrailingPunctuation(title_b,":/ "));
		
		return fields;
	}


}
