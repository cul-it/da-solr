package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

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

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class CallNumberResultSetToFields implements ResultSetToFieldsStepped {

	
	final boolean debug = false;
	
	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> callnos = new HashSet<String>();
		Collection<String> letters = new HashSet<String>();
		
		/*
		 * Step 1. 
		 * Retrieve call number list, and identify initial call number letters for facet.
		 * Build queries to retrieve subject names for the initial letters found.
		 */
				
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if (debug)
				System.out.println(resultKey);
			
			if ( resultKey.endsWith("callno")) {
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();

						if (debug) {
							System.out.println(" result.");
							Iterator<String> i = sol.varNames();
							while (i.hasNext()) {
								String fieldname = i.next();
								System.out.println(fieldname +": " + nodeToString(sol.get(fieldname)));
							}
						}
							
						String callno = nodeToString( sol.get("part1") );
						if (callno.equalsIgnoreCase("No Call Number")) continue;
						if (sol.contains("ind1") && ! nodeToString( sol.get("ind1")).equals("0")) {
							// Not an LOC call number (probably)
						} else {
							// How many letters at beginning of call number?
							int i = 0;
							while ( callno.length() > i) {
								if ( Character.isLetter(callno.substring(i, i+1).charAt(0)) )
									i++;
								else
									break;
							}
							if (i >= 1)
								letters.add( callno.substring(0,1).toUpperCase() );
							if (i > 1)
								letters.add( callno.substring(0,i).toUpperCase() );
						}
						if (sol.contains("part2")) {
							String part2 = nodeToString( sol.get("part2") );
							if (! part2.equals("")) {
								callno += " " + part2;
							}
						}
						if (debug)
							System.out.println(callno);
						if (resultKey.equals("holdings_callno") ) {
//								&& sol.contains("loc")
//								&& ! nodeToString( sol.get("loc")).equals("serv,remo")) {
							callnos.add(callno);
							if (callno.toLowerCase().startsWith("thesis ")) {
								callno = callno.substring(7);
								callnos.add(callno);
							}
							if (sol.contains("prefix")) {
								String prefix = nodeToString( sol.get("prefix") );
								if (! prefix.equals("")) {
									callno = prefix + " " + callno;
									callnos.add(callno);
								}
							}
						}
					}
				}
			}
		}

		// It has been decided not to display call numbers in the item view,
        // only in the holdings data, but all call numbers are still populated in the call number search.
 		Iterator<String> i = callnos.iterator();
		while (i.hasNext())
			addField(fields,"lc_callnum_full",i.next());
				
		i = letters.iterator();
		while (i.hasNext()) {
			String l = i.next();
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
						String l = nodeToString( sol.get("code") );
						String subject = nodeToString( sol.get("subject") );
						if (l.length() == 1)
							addField(fields,"lc_1letter_facet",l+" - "+subject);
						addField(fields,"lc_alpha_facet",l+" - "+subject);
					}
				}
			}
		}
		
		
		step.setFields(fields);
		return step;
	}


}
