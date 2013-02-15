package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class Title130ResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
						
		Set<String> resultKeys = results.keySet();
		Iterator<String> i = resultKeys.iterator();
		HashMap<String,ArrayList<String>> fieldparts = new HashMap<String,ArrayList<String>>();
		
		while (i.hasNext()) {
			String resultKey = i.next();
			ResultSet rs = results.get(resultKey);
			
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					String c = nodeToString(sol.get("code"));
					ArrayList<String> vals;
					if (fieldparts.containsKey(c)) {
						vals = fieldparts.get(c);
					} else {
						vals = new ArrayList<String>();
					}
					vals.add(nodeToString(sol.get("value")));
					fieldparts.put(c, vals);
				}
			}
		}
		if (fieldparts.size() > 0) {
			String field = processFieldpartsIntoField(fieldparts,"adghplskfmnor");
			String title_cts = processFieldpartsIntoField(fieldparts,"a");
			if (title_cts.length() > 0) {
				field += "|"+title_cts;
				addField(fields,"title_uniform_display", field);
			}
		}
		return fields;
	
	}
		
	
	public String processFieldpartsIntoField ( HashMap<String,ArrayList<String>> parts, String subfields ) {
		List<String> ordered = new ArrayList<String>();
		for (int i = 0; i < subfields.length(); i++) {
			String c = subfields.substring(i, i+1);
			if (parts.containsKey(c)) {
				ordered.addAll(parts.get(c));
			}
		}
		StringBuilder sb = new StringBuilder();
		if (ordered.size() > 0) {
			sb.append(ordered.get(0));
		}
		for (int i = 1; i < ordered.size(); i++) {
			sb.append(" ");
			sb.append(ordered.get(i));
		}
		return sb.toString();
		
	}


}
