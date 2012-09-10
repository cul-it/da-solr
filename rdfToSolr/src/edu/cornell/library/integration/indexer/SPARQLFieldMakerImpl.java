package edu.cornell.library.integration.indexer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

/**
 * FieldMaker that runs a SPARQL query and uses the results
 * to make SolrInputFields.
 * 
 * @author bdc34
 *
 */
public class SPARQLFieldMakerImpl implements FieldMaker{	
	
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
	
			
	/** Process the results of the SPARQL queries into fields */
	ResultSetToFields resultSetToFields;

	public String getName() {
		return name;
	}
	public SPARQLFieldMakerImpl setName(String name) {
		this.name = name;
		return this;
	}


	public Map<String, String> getLocalStoreQueries() {
		return localStoreQueries;
	}
	public SPARQLFieldMakerImpl setLocalStoreQueries(Map<String, String> localStoreQueries) {
		this.localStoreQueries = localStoreQueries;
		return this;
	}
	public SPARQLFieldMakerImpl addLocalStoreQuery(String key, String query){
		if( this.localStoreQueries == null )			
			this.localStoreQueries = new HashMap<String,String>();
		
		this.localStoreQueries.put(key,query);
		return this;
	}

	public Map<String, String> getMainStoreQueries() {
		return mainStoreQueries;
	}
	public SPARQLFieldMakerImpl setMainStoreQueries(Map<String, String> mainStoreQueries) {
		this.mainStoreQueries = mainStoreQueries;
		return this;
	}
	public SPARQLFieldMakerImpl addMainStoreQueries(String key, String query){
		if( this.mainStoreQueries == null )
			this.mainStoreQueries = new HashMap<String, String>();
		
		this.mainStoreQueries.put(key, query);
		return this;		
	}


	public ResultSetToFields getResultSetToFields() {
		return resultSetToFields;
	}
	public SPARQLFieldMakerImpl  setResultSetToFields(ResultSetToFields resultSetToFields) {
		this.resultSetToFields = resultSetToFields;
		return this;
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, 
			RDFQueryService mainStore,
			RDFQueryService localStore) {
		
		Map<String, ResultSet> results = new HashMap<String,ResultSet>();

		//run local queries		
		if( localStore != null ){
			Map<String,String> queries = getLocalStoreQueries();
			if( queries != null ){
				for( String queryName : queries.keySet()){			
					String query = queries.get(queryName);
					query = substitueInRecordURI( recordURI, query );
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
					String query = queries.get(queryName);
					query = substitueInRecordURI( recordURI, query );
					ResultSet rs = sparqlSelectQuery(query, mainStore);
					results.put(queryName, rs);			
				}
			}
		}
		
		return getResultSetToFields().toFields(results);
	}	
	
	private String substitueInRecordURI(String recordURI, String query) {
		if( query == null )
			return null;			
		return query.replaceAll("\\$recordURI\\$", "<"+recordURI+">");		
	}
	
	
	protected ResultSet sparqlSelectQuery(String query, RDFQueryService rdfService) {
		ResultSet resultSet = null;
		try {
			InputStream resultStream = rdfService.sparqlSelectQuery(query,
					RDFQueryService.ResultFormat.JSON);
			resultSet = ResultSetFactory.fromJSON(resultStream);
			return resultSet;
		} catch (Exception e) {
			log.error("error executing sparql select query: \n"+
					query + "\n" + e.getMessage());
		}

		return resultSet;
	}
	
	static final Log log = LogFactory.getLog( SPARQLFieldMakerImpl.class);
}
