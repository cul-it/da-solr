package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.text.SimpleDateFormat;
import java.util.Calendar;
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
 * Look for various factors to identify books on new books shelves and acquisition
 * dates for recently acquired materials. Acquisition dates are less reliable the
 * more time has passed.
 * 
 */
public class NewBooks implements ResultSetToFields {
	
	final static Boolean debug = false;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();
		
	  	Collection<String> f948as = new HashSet<>();
	  	Collection<String> loccodes = new HashSet<>();
	  	
	  	Integer twoYearsAgo = Integer.valueOf(twoYearsAgo());
	  	
	  	// Begin New Book Shelf Logic
	  	ResultSet rs = results.get("k");
	  	if (rs != null) 
	  		while (rs.hasNext()) {
		  		QuerySolution sol = rs.nextSolution();
		  		String k = nodeToString(sol.get("callnumprefix"));
	  			if (debug) System.out.println("found a k: "+k);
		  		if (k.trim().equalsIgnoreCase("new & noteworthy books")) {
		  			addField(fields,"new_shelf","Olin Library New & Noteworthy Books");
		  		}
	  		}
	  	
	  	Boolean matchingMfhd = false;
	  	Boolean newBooksZNote = false;
	  	rs = results.get("newbooksMfhd");
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
	  		if (debug) {
	  			Iterator<String> i = sol.varNames();
	  			while (i.hasNext()) System.out.println(i.next());
	  		}
	  		String code = nodeToString(sol.get("code"));
	  		loccodes.add(code);
	  		
	  		
	  		if (sol.contains("z") && sol.get("z") != null) {
	  			String z = nodeToString(sol.get("z")).toLowerCase();
	  			if (debug) System.out.println("z note: "+z);
	  			if (z.contains("new book") && z.contains("shelf")) {
	  				newBooksZNote = true;
	  				if (debug) System.out.println("Looks like the book is on a new books shelf");
	  			}
	  		}
				
	  		// the rest of this MFHD while loop relates to determining whether
	  		// the book will count as "new" (as opposed to being on a new book shelf).
	  		String x = null;
	  		if (sol.contains("x") && sol.get("x") != null) {
	  			x = nodeToString(sol.get("x"));
	  			if (x.contains("transfer")) {
	  				if (code.endsWith(",anx")) {
	  					if (debug) System.out.println("This MFHD isn't evidence of recent acquisition because it represents a tranfer to the annex. "+code+" "+x);
	  					continue;
	  				}
	  			}
	  		}
	  		String five = nodeToString(sol.get("five"));
	  		if (five.length() < 6) continue;
	  		String date = five.substring(0, 8);
	  		if (debug) System.out.println(date);
	  		if (Integer.valueOf(date) < twoYearsAgo) {
	  			if (debug) System.out.println("This MFHD isn't evidence of recent acquisition because the 005 is too old. "+five);
	  			continue;
	  		}
	  		if (debug) System.out.println("This MFHD is recent enough to argue for recent acquisition and wasn't otherwise eliminated.");
	  		matchingMfhd = true;
	  		continue;
	  	}
	  	
	  	if (newBooksZNote)
	  		for (String loccode : loccodes) {
	  			if (debug) System.out.println(loccode);
	  			if (loccode.startsWith("afr"))
		  			addField(fields,"new_shelf","Africana Library New Books Shelf");
	  		}

	  	
	  	// Begin New Books Logic
	  	
	  	if ( ! matchingMfhd ) {
	  		if (debug) System.out.println("No MFHD is recent enough (and matching other criteria) to indicate a recent acquisition.");
	  		return fields; //empty field set unless new books shelf flags found
	  	}

	  	rs = results.get("newbooks948");
	  	if (rs == null) return fields;
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
//	  		String ind1 = nodeToString(sol.get("ind1"));
//	  		if ( ! ind1.equals("1")) continue;
	  		String a = nodeToString(sol.get("a"));
	  		if (debug) System.out.println(a);
	  		if (Integer.valueOf(a) < twoYearsAgo) continue;
	  		if (debug) System.out.println("948a value is recent enough to indicate recent acquisition.");
	  		f948as.add(a);
	  	}
	  	if (f948as.size() == 0) {
	  		if (debug) System.out.println("Not a new book due to no 948a with 1st indicator 1 and date within 2 years.");
	  		return fields; //empty field set unless new books shelf flags found
	  	}

	  	Boolean isMicroform = false;
	  	rs = results.get("seven");
	  	while (rs.hasNext()) {
	  		QuerySolution sol = rs.nextSolution();
	  		String cat = nodeToString(sol.get("cat"));
	  		if (debug) System.out.println("Category from 007 field found: "+cat);
	  		if (cat.equals("h")) isMicroform = true;
	  	}
	  	if (isMicroform) {
	  		if (debug) System.out.println("Not a new book due to being microform.");
	  		return fields; //empty field set unless new books shelf flags found
	  	}
	  	
	  	Integer acquiredDate = null;
	  	for (String f948a : f948as) {
	  		Integer date = Integer.valueOf(f948a.substring(0, 8));
	  		if (acquiredDate == null || acquiredDate < date)
	  			acquiredDate = date;
	  	}
	  	if (acquiredDate != null) {
		  	if (debug) System.out.println("Of "+f948as.size()+" recent 948a's, "+acquiredDate+" seems to be the most recent.");
		  	String date_s = acquiredDate.toString();
			addField(fields,"acquired",date_s);
			addField(fields,"acquired_month",date_s.substring(0,6));
	  	}
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
