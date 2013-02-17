package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class PubInfoResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
	  	Map<String,HashMap<String,ArrayList<String>>> marcfields = 
	  			new HashMap<String,HashMap<String,ArrayList<String>>>();
						
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					String fti = nodeToString(sol.get("f")) +
							nodeToString(sol.get("t")) + 
							" " + //placeholder because we're not getting first indicator 
							nodeToString(sol.get("i2"));
					HashMap<String,ArrayList<String>> fieldparts;
					if (marcfields.containsKey(fti)) {
						fieldparts = marcfields.get(fti);
					} else {
						fieldparts = new HashMap<String,ArrayList<String>>();
					}
					String c = nodeToString(sol.get("c"));
					ArrayList<String> vals;
					if (fieldparts.containsKey(c)) {
						vals = fieldparts.get(c);
					} else {
						vals = new ArrayList<String>();
					}
					vals.add(nodeToString(sol.get("v")));
					fieldparts.put(c, vals);
					marcfields.put(fti, fieldparts);
				}
			}
		}
		
		for (String fti: marcfields.keySet()) {
			String ind = fti.substring(fti.length()-2);
//			String t = fti.substring(fti.length()-5, fti.length()-2);
			HashMap<String,ArrayList<String>> fieldparts = marcfields.get(fti);
			String relation = "";
			if (ind.endsWith("0")) relation = "pub_prod";
			else if (ind.endsWith("1")) relation = "pub_info";
			else if (ind.endsWith("2")) relation = "pub_dist";
			else if (ind.endsWith("3")) relation = "pub_manu";
			else if (ind.endsWith("4")) relation = "pub_copy";
			String val = combine_subfields("abc",fieldparts);
			if (! relation.equals("")) {
				addField(fields,relation+"_display",val);
			}
		}
		
		return fields;
	}	

	
	public String combine_subfields (String subfields, HashMap<String,ArrayList<String>> fieldparts) {
		
		List<String> ordered = new ArrayList<String>();
		for (int i = 0; i < subfields.length(); i++) {
			String c = subfields.substring(i, i+1);
			if (fieldparts.containsKey(c)) {
				ordered.addAll(fieldparts.get(c));
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
