package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class CallNumberResultSetToFields implements ResultSetToFieldsStepped {

	
	final boolean debug = false;
	
	@Override
	public FieldMakerStep toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		ArrayList<String> callnos = new ArrayList<String>();
		ArrayList<String> letters = new ArrayList<String>();
		String sort_callno = null;
		ArrayList<Classification> classes = new ArrayList<Classification>();
		String non_lc_callno = null; // We only need up to one of these
		
		/*
		 * Step 1. 
		 * Retrieve call number list, and identify initial call number letters for facet.
		 * Build queries to retrieve subject names for the initial letters found.
		 */
				
		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			if (debug)
				System.out.println(resultKey);

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
						if (non_lc_callno == null) {
							if (sol.contains("part2"))
								non_lc_callno = callno+" "+nodeToString( sol.get("part2") );
							else
								non_lc_callno = callno;
						}
					} else {
						// How many letters at beginning of call number?
						int i = 0;
						while ( callno.length() > i) {
							if ( Character.isLetter(callno.charAt(i)) )
								i++;
							else
								break;
						}
						if (i <= 3) { // Too many letters suggests not an LC call no
							if (i >= 1)
								letters.add( callno.substring(0,1).toUpperCase() );
							if (i > 1)
								letters.add( callno.substring(0,i).toUpperCase() );
							if (callno.length() > i) {
								int j = i;
								for ( ; j < callno.length() ; j++) {
									Character c = callno.charAt(j);
									if (! Character.isDigit(c) && ! c.equals('.'))
										break;
								}
								classes.add(new Classification(
										callno.substring(0,i).toUpperCase(),
										callno.substring(i, j)));
							}
							// There can be only one. Holdings take precedence.
							if (sort_callno == null || resultKey.startsWith("holdings")) {
								if (sol.contains("part2"))
									sort_callno = callno+" "+nodeToString( sol.get("part2") );
								else
									sort_callno = callno;
							}
						}
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
//							&& sol.contains("loc")
//							&& ! nodeToString( sol.get("loc")).equals("serv,remo")) {
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
		if (sort_callno == null)
			sort_callno = non_lc_callno;

		// It has been decided not to display call numbers in the item view,
        // only in the holdings data, but all call numbers are still populated in the call number search.
		for (int i = 0; i < callnos.size(); i++)
			addField(fields,"lc_callnum_full",callnos.get(i));
		
		if (sort_callno != null) 
			addField(fields,"callnum_sort",sort_callno);
				
		for (int i = 0; i < letters.size(); i++) {
			String l = letters.get(i);
			String query = 
		    		"SELECT ?code ?subject\n" +
		    		"WHERE {\n" +
		    		" ?lc intlayer:code \""+l+"\".\n" +
		    		" ?lc intlayer:code ?code.\n" +
		    		" ?lc rdfs:label ?subject. }\n";
			step.addMainStoreQuery("letter_subject_"+l,query );
		}
		
		// new bl5-compatible hierarchical facet
		int classCount = classes.size();
		Collection<String> lc_callnum_facet = new HashSet<String>(); // hashset eliminates dupes
		if ( classCount != 0 ) {
			try (   Connection conn = config.getDatabaseConnection("CallNos");
					PreparedStatement pstmt = conn.prepareStatement
							("SELECT label FROM classification"
							+ " WHERE ? BETWEEN low_letters AND high_letters"
							+ "   AND ? BETWEEN low_numbers AND high_numbers"
							+ " ORDER BY high_letters DESC, high_numbers DESC")  ) {

				for ( int i = 0; i < classCount; i++ ) {
					Classification c = classes.get(i);
					pstmt.setString(1, c.letters());
					pstmt.setString(2, c.numbers());
					pstmt.execute();
					StringBuilder sb = new StringBuilder();
					try (  java.sql.ResultSet rs = pstmt.getResultSet() ) {

						while (rs.next()) {
							if (sb.length() > 0)
								sb.append(":");
							sb.append(rs.getString("label"));
							lc_callnum_facet.add(sb.toString());
						}
					}
				}
			}
			for (String facetVal : lc_callnum_facet )
				addField(fields,"lc_callnum_facet",facetVal);
		}
		
		/*
		 * Step 2
		 * Add facet fields by concatenating initial letters with their subject names.
		 */
		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			
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
	
	private class Classification {
		public Classification (String letters, String numbers) {
			l = letters;
			n = numbers;
		}
		public String letters() { return l; }
		public String numbers() { return n; }
		private String l = null;
		private String n = null;
	}


}
