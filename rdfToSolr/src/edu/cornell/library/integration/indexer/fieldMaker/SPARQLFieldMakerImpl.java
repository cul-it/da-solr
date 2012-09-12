package edu.cornell.library.integration.indexer.fieldMaker;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;

import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;

import static edu.cornell.library.integration.indexer.IndexingUtilities.*;

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
	List<ResultSetToFields> resultSetToFields;
	
	
	Map<String,String> defaultPrefixes;
	
	public SPARQLFieldMakerImpl() {
		this.defaultPrefixes = new HashMap<String,String>();
		defaultPrefixes.put("marcrdf", "http://marcrdf.library.cornell.edu/canonical/0.1/");
	}
	
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
	public SPARQLFieldMakerImpl addMainStoreQuery(String key, String query){
		if( this.mainStoreQueries == null )
			this.mainStoreQueries = new HashMap<String, String>();
		this.mainStoreQueries.put(key, query);
		return this;		
	}


	public List<ResultSetToFields> getResultSetToFields() {
		return resultSetToFields;
	}
	public SPARQLFieldMakerImpl  setResultSetToFieldsList(List<ResultSetToFields> resultSetToFieldsList) {
		this.resultSetToFields = resultSetToFieldsList;
		return this;
	}
	public SPARQLFieldMakerImpl  addResultSetToFields(ResultSetToFields resultSetToFields) {
		if( this.resultSetToFields == null )
			this.resultSetToFields = new ArrayList<ResultSetToFields>();		
		this.resultSetToFields.add( resultSetToFields );
		return this;
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, 
			RDFQueryService mainStore,
			RDFQueryService localStore) throws Exception {
		
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
					StringBuilder querybuild = new StringBuilder();
					if (! this.defaultPrefixes.isEmpty()) {
						for( String prefix : this.defaultPrefixes.keySet() ) {
							querybuild.append("PREFIX " + prefix + ":  <" + this.defaultPrefixes.get(prefix) + ">\n");
						}
					}
					querybuild.append(substitueInRecordURI( recordURI, queries.get(queryName) ));
					String query = querybuild.toString();
					ResultSet rs = sparqlSelectQuery(query, mainStore);
					results.put(queryName, rs);			
				}
			}
		}
		
		return toSolrFields( results );
	}	
	
	
	Map<? extends String, ? extends SolrInputField> toSolrFields( Map<String, ResultSet> results ) throws Exception{
		Map<String, SolrInputField> fields = new HashMap<String,SolrInputField>();
		for( ResultSetToFields r2f : getResultSetToFields() ){
			fields.putAll( r2f.toFields( results ) );
		}
		return fields;
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
