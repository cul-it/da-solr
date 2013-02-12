package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class URLResultSetToFields implements ResultSetToFields {

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
					String fi = nodeToString(sol.get("f")) +
							nodeToString(sol.get("i1")) + 
							nodeToString(sol.get("i2"));
					HashMap<String,ArrayList<String>> fieldparts;
					if (marcfields.containsKey(fi)) {
						fieldparts = marcfields.get(fi);
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
					marcfields.put(fi, fieldparts);
				}
			}
		}
		
		for (String fti: marcfields.keySet()) {
			String ind = fti.substring(fti.length()-2);
			String relation ="access"; //this is a default and may change later
/*			if ((ind.equals("40")) || (ind.equals("41"))) {
				relation = "access";
			} else if (ind.equals("42"))  {
				if (marcfields.get(fti).containsKey("3")) {
					ArrayList<String> threes = marcfields.get(fti).get("3");
					for (String three: threes) {
						if (three.toLowerCase().contains("table of contents")) 
							relation = "toc";
					}
				}
			} */ 
			if (relation.equals("")) relation = "other";
			HashMap<String,ArrayList<String>> fieldparts = marcfields.get(fti);
			if ( ! fieldparts.containsKey("u")) continue;
			ArrayList<String> us = fieldparts.get("u");
			ArrayList<String> threes = new ArrayList<String>();
			ArrayList<String> zs = new ArrayList<String>();
			if (fieldparts.containsKey("3")) threes = fieldparts.get("3");
			if (fieldparts.containsKey("z")) zs = fieldparts.get("z");
			StringBuilder sb = new StringBuilder();
			for (String three: threes) {
				sb.append(" ");
				sb.append(three);
			}
			for (String z: zs) {
				sb.append(" ");
				sb.append(z);
			}
			String comment = sb.toString().trim();
			String lc_comment = comment.toLowerCase();
			if (lc_comment.contains("table of contents")
					|| lc_comment.contains("tables of contents")
					|| lc_comment.endsWith(" toc")
					|| lc_comment.contains(" toc ")
					|| lc_comment.startsWith("toc ")
					|| lc_comment.equals("toc")
					|| lc_comment.contains("cover image")
					|| lc_comment.contains("publisher description")
					|| lc_comment.contains("contributor biographical information")
					|| lc_comment.contains("sample text")) {
				relation = "other";
			}
			for (String u: us) {
				if (u.toLowerCase().contains("://plates.library.cornell.edu")) {
					relation = "bookplate";
					if (! comment.equals(""))
						addField(fields,"donor_s",comment);
				}
				if (comment.equals("")) {
					addField(fields,"url_"+relation+"_display",u);						
				} else {
					addField(fields,"url_"+relation+"_display",u + "|" + comment);
				}
			}
		}
		
		return fields;
	}	

}
