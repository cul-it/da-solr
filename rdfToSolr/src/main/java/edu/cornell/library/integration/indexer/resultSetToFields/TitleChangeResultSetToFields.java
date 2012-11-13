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
public class TitleChangeResultSetToFields implements ResultSetToFields {

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
			String relation ="";
			if (t.equals("780")) {
				if (ind.endsWith("0"))/**/ {
					relation = "continues";
				} else if (ind.endsWith("1"))/**/ {
					relation = "continues_in_part";
				} else if (ind.endsWith("2") || ind.endsWith("3")) {
					relation = "supersedes";
				} else if (ind.endsWith("4"))/**/ {
					relation = "merger_of";
				} else if (ind.endsWith("5"))/**/ {
					relation = "absorbed";
				} else if (ind.endsWith("6"))/**/ {
					relation = "absorbed_in_part";
				} else if (ind.endsWith("7"))/**/ {
					relation = "separated_from";
				}
			} else if (t.equals("785")) {
				if (ind.endsWith("0"))/**/ {
					relation = "continued_by";
				} else if (ind.endsWith("1"))/**/ {
					relation = "continued_in_part_by";
				} else if (ind.endsWith("2") || ind.endsWith("3")) {
					relation = "superseded_by";
				} else if (ind.endsWith("4"))/**/  {
					relation = "absorbed_by";
				} else if (ind.endsWith("5"))/**/ {
					relation = "absorbed_in_part_by";
				} else if (ind.endsWith("6"))/**/ {
					relation = "split_into";
				} else if (ind.endsWith("7"))/**/ {
					relation = "merger"; //Should never display from 785
				}
			} else if (t.equals("765")) {
				relation = "translation_of";
			} else if (t.equals("767")) {
				relation = "has_translation";
			} else if (t.equals("775")) {
				relation = "other_edition";
			} else if (t.equals("770")) {
				relation = "has_supplement";
			} else if (t.equals("772")) {
				relation = "supplement_to";
			} else if (t.equals("776")) {
				relation = "other_form";
			} else if (t.equals("777")) {
				relation = "issued_with";
			}
			HashMap<String,ArrayList<String>> fieldparts = marcfields.get(fti);
			String clicktosearch = combine_subfields("t",fieldparts);
			if (clicktosearch.length() < 2) {
				continue;				
			}
			if (! relation.equals("")) {
				if (ind.startsWith("0")) {
					String displaystring = combine_subfields("iatbcdgkqrsw", fieldparts);
					addField(fields,relation+"_display",displaystring + '|'+ clicktosearch );
				}
			}
			if (t.equals("780") 
					|| t.equals("785")
					|| t.equals("765")
					|| t.equals("767")
					|| t.equals("775")
					|| t.equals("770")
					|| t.equals("772")
					|| t.equals("776")
					|| t.equals("777")
					) {
				String subfields = "atbcdegkqrs";
				addField(fields,"title_uniform_t",combine_subfields(subfields, fieldparts));
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
