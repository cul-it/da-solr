package edu.cornell.library.integration.indexer.resultSetToFields;

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
public class LanguageResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> facet_langs = new HashSet<String>();
		Collection<String> display_langs = new HashSet<String>();
		
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					if (resultKey.equals("language_main")) {
						display_langs.add(nodeToString(sol.get("language")));
						facet_langs.add(nodeToString(sol.get("language")));						
					} else if (resultKey.equals("language_041")) {
						String subfield_code = nodeToString(sol.get("c"));
						display_langs.add(nodeToString(sol.get("language")));
						if ("adegj".contains(subfield_code)) {
							facet_langs.add(nodeToString(sol.get("language")));
						}
					}
				}
			}
		}
		
		Iterator<String> i = facet_langs.iterator();
		while (i.hasNext()) addField(fields,"language_facet",i.next());
		StringBuilder sb = new StringBuilder();
		i = display_langs.iterator();
		Boolean first = true;
		while (i.hasNext()) {
			if (first) first = false;
			else sb.append(", ");
			sb.append(i.next());
		}
		addField(fields, "language_display",sb.toString());
		
		return fields;
	}


}
