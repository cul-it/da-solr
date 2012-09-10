package edu.cornell.library.integration.indexer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * FieldMaker that runs a SPARQL query and uses the results
 * to make SolrInputFields.
 * 
 * @author bdc34
 *
 */
public abstract class SPARQLFieldMakerImpl implements FieldMaker{
	
	Log log = LogFactory.getLog( SPARQLFieldMakerImpl.class);
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, 
			RDFQueryService mainStore,
			RDFQueryService localStore) {
		
		Map<String, ResultSet> results = new HashMap<String,ResultSet>();

		//run local queries
		Map<String,String> queries = getLocalStoreQueries();
		for( String queryName : queries.keySet()){			
			String query = queries.get(queryName);
			ResultSet rs = sparqlSelectQuery(query, localStore);
			results.put(queryName, rs);			
		}
		
		//run remote queries
		queries = getMainStoreQueries();
		for( String queryName : queries.keySet()){			
			String query = queries.get(queryName);
			ResultSet rs = sparqlSelectQuery(query, mainStore);
			results.put(queryName, rs);			
		}		
		
		return fieldsForResults( results );
	}

	/** Human readable name for this FieldMaker. Used in debugging */
	public abstract String name();
	
	/** Gets the SPARQL queries to run.  These get run against
	 * the local store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	public abstract Map<String,String> getLocalStoreQueries();
	
	/** Gets the SPARQL queries to run. These get run against the
	 * main store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	public abstract Map<String,String> getMainStoreQueries();

	/** Process the results of the SPARQL queries into fields */  
	public abstract Map<? extends String, ? extends SolrInputField>
		fieldsForResults(Map<String,ResultSet> resultSets );
	
	
	protected ResultSet sparqlSelectQuery(String query, RDFQueryService rdfService) {
	    	
		ResultSet resultSet = null;

		try {
			InputStream resultStream = rdfService.sparqlSelectQuery(query,
					RDFQueryService.ResultFormat.JSON);
			resultSet = ResultSetFactory.fromJSON(resultStream);
			return resultSet;
		} catch (RDFServiceException e) {
			log.error("error executing sparql select query: " + e.getMessage());
		}

		return resultSet;
	}
}
