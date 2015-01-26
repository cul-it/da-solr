package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class NewBooksRSTF implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		
	  	Collection<String> f948as = new HashSet<String>();
	  	
	  	Integer twoYearsAgo = Integer.valueOf(twoYearsAgo());
	  	
	  	// Begin New Book Shelf Logic
	  	ResultSet rs = results.get("k");
	  	if (rs != null) 
	  		while (rs.hasNext()) {
		  		QuerySolution sol = rs.nextSolution();
		  		String k = nodeToString(sol.get("callnumprefix"));
		  		if (k.trim().equalsIgnoreCase("new & noteworthy"))
		  			addField(fields,"new_shelf","Olin Library New & Noteworthy Books");
	  		}
	  	
	  	
	  	// Begin New Books Logic
	  	rs = results.get("newbooks948");
	  	if (rs == null) return fields;
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
	  		String ind1 = nodeToString(sol.get("ind1"));
	  		if ( ! ind1.equals("1")) continue;
	  		String a = nodeToString(sol.get("a"));
	  		if (Integer.valueOf(a) < twoYearsAgo) continue;
	  		f948as.add(a);
	  	}
	  	if (f948as.size() == 0) return fields; //empty field set

	  	Boolean isMicroform = false;
	  	rs = results.get("seven");
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
	  		String cat = nodeToString(sol.get("cat"));
	  		if (cat.equals("h")) isMicroform = true;
	  	}
	  	if (isMicroform) return fields; //empty field set
	  	
	  	Boolean matchingMfhd = false;
	  	rs = results.get("newbooksMfhd");
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
	  		String five = nodeToString(sol.get("five"));
	  		if (five.length() < 6) continue;
	  		String date = five.substring(0, 8);
	  		String x = null, code = null;
	  		if (Integer.valueOf(date) < twoYearsAgo) continue;
	  		if (sol.contains("x") && sol.get("x") != null) {
	  			x = nodeToString(sol.get("x"));
	  			if (x.contains("transfer")) {
	  				code = nodeToString(sol.get("code"));
	  				if (code.endsWith(",anx")) continue;
	  			}
	  		}
	  		matchingMfhd = true;
	  		break;
	  	}
	  	if ( ! matchingMfhd ) return fields; //empty field set
	  	
	  	Integer acquiredDate = null;
	  	for (String f948a : f948as) {
	  		Integer date = Integer.valueOf(f948a.substring(0, 8));
	  		if (acquiredDate == null || acquiredDate < date)
	  			acquiredDate = date;
	  	}
	  	
	  	String date_s = acquiredDate.toString();
		addField(fields,"acquired",date_s);
		addField(fields,"acquired_month",date_s.substring(0,6));

		return fields;
	}
	
    public static String twoYearsAgo(  ) {
    	Calendar now = Calendar.getInstance();
    	String thisYear = new SimpleDateFormat("yyyy").format(now.getTime());
    	Integer twoYearsAgo = Integer.valueOf(thisYear) - 2;
    	String thisMonthDay = new SimpleDateFormat("MMdd").format(now.getTime());
    	return twoYearsAgo.toString() + thisMonthDay;
    }
}
