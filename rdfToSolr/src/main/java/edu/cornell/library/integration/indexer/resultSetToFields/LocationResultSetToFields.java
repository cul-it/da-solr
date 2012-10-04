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
public class LocationResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> facets = new HashSet<String>();
		Collection<String> displays = new HashSet<String>();
		
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					displays.add(nodeToString(sol.get("location_name")));
					facets.add(nodeToString(sol.get("library_name")));
					facets.add(nodeToString(sol.get("group_name")));
				}
			}
		}

		Iterator<String> i = facets.iterator();
		while (i.hasNext()) addField(fields,"location_facet",i.next());
		i = displays.iterator();
		while (i.hasNext()) addField(fields,"location_display",i.next());
		
		return fields;
	}


}
