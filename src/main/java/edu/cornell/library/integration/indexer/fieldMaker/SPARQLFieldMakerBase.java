package edu.cornell.library.integration.indexer.fieldMaker;

import static edu.cornell.library.integration.utilities.IndexingUtilities.substituteInRecordURI;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

public abstract class SPARQLFieldMakerBase implements FieldMaker{

	/** Human readable name for this FieldMaker. Used in debugging */
	String name;

	/** Gets the SPARQL queries to run. These get run against the
	 * main store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	Map<String,String> queries;

	Map<String,String> defaultPrefixes;

	protected boolean debug = false;
	
	public SPARQLFieldMakerBase() {
		this.defaultPrefixes = new HashMap<>();
		defaultPrefixes.put("marcrdf", "http://marcrdf.library.cornell.edu/canonical/0.1/");
		defaultPrefixes.put("intlayer","http://da-rdf.library.cornell.edu/integrationLayer/0.1/");
		defaultPrefixes.put("rdfs",    "http://www.w3.org/2000/01/rdf-schema#");		                           
		defaultPrefixes.put("rdf",     "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		defaultPrefixes.put("xsd",     "http://www.w3.org/2001/XMLSchema#");
		
	}
	
	public String getName() {
		return name;
	}
	// setName(String name) is in implementations for fluent style

	public Map<String, String> getQueries() {
		return queries;
	}

	/**
	 * Substitute in recordURI to queries and run them against their stores, then
	 * return all the result sets. This method works with the assumption that the 
	 * query strings do not have the recordURI substituted in.
	 */
	protected Map<String,ResultSet> runQueries( 
			String recordURI, 
			Config config) throws Exception {
		Map<String, ResultSet> results = new HashMap<>();

		//run remote queries
		RDFService triplestore = config.getRDFService("batch");
		if( triplestore != null ){
			Map<String,String> queries = getQueries();
			if( queries != null){
				for( String queryName : queries.keySet()){
					StringBuilder querybuild = new StringBuilder();
					if (! this.defaultPrefixes.isEmpty()) {
						for( String prefix : this.defaultPrefixes.keySet() ) {
							querybuild.append("PREFIX " + prefix + ":  <" + this.defaultPrefixes.get(prefix) + ">\n");
						}
					}
					querybuild.append(substituteInRecordURI( recordURI, queries.get(queryName) ));
					String query = querybuild.toString();
					debugRemoteQuery( query );
					ResultSet rs = sparqlSelectQuery(query, triplestore);
					if (debug && query.contains("loccode")) {
						try ( InputStream is = triplestore.sparqlSelectQuery(query, RDFService.ResultFormat.TEXT) ) {
							String bib_xml = convertStreamToString(is);
							System.out.println(bib_xml);
						}
					}
					results.put(queryName, rs);			
				}
			}
		}
		return results;
	}
	
	public static String convertStreamToString(java.io.InputStream is) {
		String answer = null;
		try (java.util.Scanner scanner = new java.util.Scanner(is)) {
			scanner.useDelimiter("\\A");
			answer =  scanner.hasNext() ? scanner.next() : "";
		}
	    return answer;
	}

	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, 
			Config config) throws Exception {
		
		Map<String, ResultSet> resultSets = runQueries(recordURI, config);  

		return resultSetsToSolrFields( resultSets, config );				  
	}	
	
	
	private void debugRemoteQuery(String query) {
		if( debug ) {
			if (query.contains("loccode"))
				System.out.println("Remote query for " + getName() + ":'" + query + "'");
		}
	}

	/**
	 * Convert the result sets generated from running the SPARQL queries to
	 * SolrInputFields.
	 */
	protected abstract Map<? extends String, ? extends SolrInputField> 
	    resultSetsToSolrFields( Map<String, ResultSet> results, Config config ) 
			throws Exception;


	protected static ResultSet sparqlSelectQuery(String query, RDFService rdfService) throws Exception {
		ResultSet resultSet = null;
		try ( InputStream resultStream = rdfService.sparqlSelectQuery(query,RDFService.ResultFormat.JSON) ) {
			resultSet = ResultSetFactory.fromJSON(resultStream);
		} catch (Exception e) {
			throw new Exception("error executing sparql select query: \n"+
					query + "\n" + e.getMessage(),e);
		}
		return resultSet;
	}
	

}
