package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
public class LocationResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();
		Collection<String> facets = new HashSet<>();
		Collection<String> displays = new HashSet<>();
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					displays.add(nodeToString(sol.get("location_name")));
					String libname = nodeToString(sol.get("library_name"));
					if ( ! libname.equals("") ) {
						facets.add(libname);
						addField(fields,"online","At the Library");
					}
//					facets.add(nodeToString(sol.get("group_name")));
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
