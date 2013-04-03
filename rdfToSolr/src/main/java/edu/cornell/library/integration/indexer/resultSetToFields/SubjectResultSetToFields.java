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
public class SubjectResultSetToFields implements ResultSetToFields {

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
							nodeToString(sol.get("i1")) + 
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
			String t = fti.substring(fti.length()-5, fti.length()-2);
			String main_fields = "", dashed_fields = "";
			if (t.equals("600")) {
				main_fields = "abcdefghkjlmnopqrstu";
				dashed_fields = "vxyz";
			} else if (t.equals("610")) {
				main_fields = "abcdefghklmnoprstu";
				dashed_fields = "vxyz";
			} else if (t.equals("611")) {
				main_fields = "acdefghklnpqstu";
				dashed_fields = "vxyz";
			} else if (t.equals("630")) {
				main_fields = "adfghklmnoprst";
				dashed_fields = "vxyz";
			} else if (t.equals("650")) {
				main_fields = "abcd";
				dashed_fields = "vxyz";
			} else if (t.equals("651")) {
				main_fields = "a";
				dashed_fields = "vxyz";
			} else if (t.equals("655")) {
				if (ind.endsWith("7")) {
					main_fields = "a";
					dashed_fields = "vxyz";
				}
			} else if (t.equals("690")) {
				main_fields = "abvxyz";
			} else if (t.equals("691")) {
				main_fields = "abvxyz";
			} else if ((t.equals("692")) || (t.equals("693")) ||
					(t.equals("694")) || (t.equals("695")) ||
					(t.equals("696")) || (t.equals("697")) ||
					(t.equals("698")) || (t.equals("699"))) {
				main_fields = "a";
			}
			if (! main_fields.equals("")) {
				HashMap<String,ArrayList<String>> fieldparts = marcfields.get(fti);
				List<String> ordered = new ArrayList<String>();
				for (int i = 0; i < main_fields.length(); i++) {
					String c = main_fields.substring(i, i+1);
					if (fieldparts.containsKey(c))
						ordered.addAll(fieldparts.get(c));
				}
				StringBuilder sb = new StringBuilder();
				if (ordered.size() > 0)
					sb.append(ordered.get(0).trim());
				for (int i = 1; i < ordered.size(); i++) {
					sb.append(" ");
					sb.append(ordered.get(i).trim());
				}
				List<String> ordered_dashed = new ArrayList<String>();
				ordered_dashed.add(sb.toString());
				for (int i = 0; i < dashed_fields.length(); i++ ) {
					String c = dashed_fields.substring(i, i+1);
					if (fieldparts.containsKey(c))
						ordered_dashed.addAll(fieldparts.get(c));
				}
				sb = new StringBuilder();
				if (ordered_dashed.size() > 0) 
					sb.append(ordered_dashed.get(0));
				for (int i = 1; i < ordered_dashed.size(); i++) {
					sb.append("|");
					sb.append(ordered_dashed.get(i).trim());
				}
				addField(fields,"subject_display",RemoveTrailingPunctuation(sb.toString(),"."));
			}
		}
		
		return fields;
	}	

}
