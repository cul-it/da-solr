package edu.cornell.library.integration.indexer.fieldMaker;

import static edu.cornell.library.integration.indexer.IndexingUtilities.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;

import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

public abstract class SPARQLFieldMakerBase implements FieldMaker{
	
	/** Human readable name for this FieldMaker. Used in debugging */
	String name;
	
	/** Gets the SPARQL queries to run.  These get run against
	 * the local store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	Map<String,String> localStoreQueries;
	
	/** Gets the SPARQL queries to run. These get run against the
	 * main store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	Map<String,String> mainStoreQueries;
	
	
	Map<String,String> defaultPrefixes;

	protected boolean debug;
	
	public SPARQLFieldMakerBase() {
		this.defaultPrefixes = new HashMap<String,String>();
		defaultPrefixes.put("marcrdf", "http://marcrdf.library.cornell.edu/canonical/0.1/");
		defaultPrefixes.put("intlayer","http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/");
		defaultPrefixes.put("rdfs",    "http://www.w3.org/2000/01/rdf-schema#");		                           
		defaultPrefixes.put("rdf",     "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		defaultPrefixes.put("xsd",     "http://www.w3.org/2001/XMLSchema#");
		
	}
	
	public String getName() {
		return name;
	}
	// setName(String name) is in implementations for fluent style


	public Map<String, String> getLocalStoreQueries() {
		return localStoreQueries;
	}
	public void setLocalStoreQueries(Map<String, String> localStoreQueries) {
		this.localStoreQueries = localStoreQueries;		
	}	

	public Map<String, String> getMainStoreQueries() {
		return mainStoreQueries;
	}
	public void setMainStoreQueries(Map<String, String> mainStoreQueries) {
		this.mainStoreQueries = mainStoreQueries;		
	}

	
	/**
	 * Substitute in recordURI to queries and run them against their stores, then
	 * return all the result sets. This method works with the assumption that the 
	 * query strings do not have the recordURI substituted in. 
	 * 
	 */
	protected Map<String,ResultSet> runQueries( 
			String recordURI, 
			RDFService mainStore,
			RDFService localStore,
			Map<String,String>localQueries,
			Map<String,String>remoteQueries) throws Exception {
		Map<String, ResultSet> results = new HashMap<String,ResultSet>();

		//run local queries		
		if( localStore != null ){
			Map<String,String> queries = getLocalStoreQueries();
			if( queries != null ){
				for( String queryName : queries.keySet()){			
					String query = queries.get(queryName);
					query = substitueInRecordURI( recordURI, query );
					debugLocalQuery( query );
					ResultSet rs = sparqlSelectQuery(query, localStore);
					results.put(queryName, rs);			
				}
			}
		}
		
		//run remote queries
		if( mainStore != null ){
			Map<String,String> queries = getMainStoreQueries();
			if( queries != null){
				for( String queryName : queries.keySet()){
					StringBuilder querybuild = new StringBuilder();
					if (! this.defaultPrefixes.isEmpty()) {
						for( String prefix : this.defaultPrefixes.keySet() ) {
							querybuild.append("PREFIX " + prefix + ":  <" + this.defaultPrefixes.get(prefix) + ">\n");
						}
					}
					querybuild.append(substitueInRecordURI( recordURI, queries.get(queryName) ));
					String query = querybuild.toString();
					debugRemoteQuery( query );
					ResultSet rs = sparqlSelectQuery(query, mainStore);
					results.put(queryName, rs);			
				}
			}
		}
		return results;
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, 
			RDFService mainStore,
			RDFService localStore) throws Exception {
		
		Map<String, ResultSet> resultSets = runQueries(recordURI, mainStore, localStore, 
				  getLocalStoreQueries(), getMainStoreQueries());  

		return resultSetsToSolrFields( resultSets );				  
	}	
	
	
	private void debugRemoteQuery(String query) {
		if( debug )
			System.out.println("Remote query for " + getName() + ":'" + query + "'");
	}

	private void debugLocalQuery(String query) {
		if( debug )
			System.out.println("Local query for " + getName() + ":'" + query + "'");		
	}

	/**
	 * Convert the result sets generated from running the SPARQL queries to
	 * SolrInputFields. 
	 */
	protected abstract Map<? extends String, ? extends SolrInputField> 
	    resultSetsToSolrFields( Map<String, ResultSet> results ) 
			throws Exception;
	
	
	protected ResultSet sparqlSelectQuery(String query, RDFService rdfService) throws Exception {
		ResultSet resultSet = null;
		try {
			InputStream resultStream = rdfService.sparqlSelectQuery(query,RDFService.ResultFormat.JSON);			
			resultSet = ResultSetFactory.fromJSON(resultStream);
			return resultSet;
		} catch (Exception e) {
			throw new Exception("error executing sparql select query: \n"+
					query + "\n" + e.getMessage(),e);
		}
	}
	

}
