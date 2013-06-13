package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

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
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class CallNumberResultSetToFields implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> callnos = new HashSet<String>();
		Collection<String> callno_1stblocks = new HashSet<String>();
		Collection<String> letters = new HashSet<String>();
		
		/*
		 * Step 1. 
		 * Retrieve call number list, and identify initial call number letters for facet.
		 * Build queries to retrieve subject names for the initial letters found.
		 */
				
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			
			if ( resultKey.endsWith("callno")) {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String callno = nodeToString( sol.get("part1") );
						if (callno.startsWith("MLC")) continue;
						if (callno.equalsIgnoreCase("No Call Number")) continue;
						if (sol.contains("ind1") && ! nodeToString( sol.get("ind1")).equals("0")) {
							// Not an LOC call number (probably)
						} else {
							if (callno.length() >= 1)
								letters.add( callno.substring(0,1) );
						}
						callno_1stblocks.add(callno);
						if (sol.contains("part2")) {
							String part2 = nodeToString( sol.get("part2") );
							if (! part2.equals("")) {
								callno += " " + part2;
							}
						}
						callnos.add(callno);
					}
				}
			}
		}

		// It has been decided not to display call numbers in the item view,
        // only in the holdings data, but all call numbers are still populated in the call number search.
 		Iterator<String> i = callnos.iterator();
		while (i.hasNext())
			addField(fields,"lc_callnum_full",i.next());
		i = callno_1stblocks.iterator();
		while (i.hasNext())
			addField(fields,"lc_callnum_main",i.next());
		
		i = letters.iterator();
		while (i.hasNext()) {
			String l = i.next();
			if (! Character.isLetter(l.charAt(0)))
				continue;
			String query = 
		    		"SELECT ?code ?subject\n" +
		    		"WHERE {\n" +
		    		" ?lc intlayer:code \""+l+"\".\n" +
		    		" ?lc intlayer:code ?code.\n" +
		    		" ?lc rdfs:label ?subject. }\n";
			step.addMainStoreQuery("letter_subject_"+l,query );
		}
		
		/*
		 * Step 2
		 * Add facet fields by concatenating initial letters with their subject names.
		 */
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			
			if ( resultKey.startsWith("letter_subject")) {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String letter = nodeToString( sol.get("code") );
						String subject = nodeToString( sol.get("subject") );
						addField(fields,"lc_1letter_facet",letter+" - "+subject);
					}
				}
			}
		}
		
		
		step.setFields(fields);
		return step;
	}


}
