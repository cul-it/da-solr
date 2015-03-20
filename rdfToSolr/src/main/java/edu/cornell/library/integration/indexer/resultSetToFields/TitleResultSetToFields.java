package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.getSortHeading;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class TitleResultSetToFields implements ResultSetToFields {
	
	private Boolean debug = false;

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		
		String title_a = null;
		String title_b = "";
		Integer ind2 = null;
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					if (debug) System.out.println( sol.toString());
					if ( ind2 == null ) {
						String ind2string = nodeToString(sol.get("ind2"));
						if (debug) System.out.println(ind2string);
						if (Character.isDigit(ind2string.charAt(0))) {
							Integer offset = Integer.valueOf(ind2string);
							if (offset > 0) 
								ind2 = offset;
						}
					}
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
		
		if ( ind2 == null ) ind2 = 0;
		
		if( title_a != null && title_b != null && title_a != null ){
			String sort_title;
			if (ind2 > title_a.length()) {
				sort_title = title_a;
			} else {
				sort_title = title_a.substring(ind2);
			}
			String clean_title_a = getSortHeading(sort_title.toLowerCase());
			addField(fields,"title_sort", clean_title_a + " " + getSortHeading(title_b));
			clean_title_a = clean_title_a.replaceAll("\\W", "").replaceAll("[^a-z]", "1");
			if (clean_title_a.length() >= 2) {
				addField(fields,"title_1letter_s",clean_title_a.substring(0,1));
				addField(fields,"title_2letter_s",clean_title_a.substring(0,2));
			} else if (clean_title_a.length() == 1) {
				addField(fields,"title_1letter_s",clean_title_a.substring(0,1));
			}
		}
		
		return fields;
	}


}
