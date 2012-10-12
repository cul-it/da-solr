package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

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
public class TitleSeriesResultSetToFields implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
						
		Set<String> resultKeys = results.keySet();
		Iterator<String> i = resultKeys.iterator();
		String tag = "";
		
		while (i.hasNext()) {
			String resultKey = i.next();
			tag = resultKey.substring(resultKey.length()-3);
			ResultSet rs = results.get(resultKey);
			HashMap<String,ArrayList<String>> fieldparts = new HashMap<String,ArrayList<String>>();
			
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					String c = nodeToString(sol.get("c"));
					ArrayList<String> vals;
					if (fieldparts.containsKey(c)) {
						vals = fieldparts.get(c);
					} else {
						vals = new ArrayList<String>();
					}
					vals.add(nodeToString(sol.get("v")));
					fieldparts.put(c, vals);
				}
			}
			if (fieldparts.size() > 0) {
				String field = processFieldpartsIntoField(fieldparts,"adfghklmnoprstv");
				addField(fields,"title_series_display", field);
				step.setFields(fields);
				return step;
			}
		}

		String nexttag = "";
		if      (tag.equals("830")) { nexttag = "490"; } 
		else if (tag.equals("490")) { nexttag = "440"; }
		else if (tag.equals("440")) { nexttag = "400"; }
		else if (tag.equals("400")) { nexttag = "410"; }
		else if (tag.equals("410")) { nexttag = "411"; }
		else if (tag.equals("411")) { nexttag = "800"; }
		else if (tag.equals("800")) { nexttag = "810"; }
		else if (tag.equals("810")) { nexttag = "811"; } 

		if (nexttag.equals("")) {
			return step;
		} else {
			step.addMainStoreQuery("title_series_"+nexttag, 
	        	"SELECT *\n" +
	        	" WHERE {\n" +
	        	"  $recordURI$ marcrdf:hasField ?f.\n" +
	        	"  ?f marcrdf:tag \""+ nexttag + "\".\n" +
	        	"  ?f marcrdf:hasSubfield ?sf.\n" +
	        	"  ?sf marcrdf:code ?c.\n" +
	        	"  ?sf marcrdf:value ?v. }");
			return step;
		}
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
