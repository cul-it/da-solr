package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.resultSetToFieldsStepped.*;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

public class SPARQLFieldMakerStepped extends SPARQLFieldMakerBase{
	
	List<ResultSetToFieldsStepped> resultSetToFieldsStepped;
	Map<String, SolrInputField> fields = new HashMap<String,SolrInputField>();

	public SPARQLFieldMakerStepped setName(String name){
		super.name = name;
		return this;
	}
	
	public SPARQLFieldMakerStepped addLocalStoreQuery(String key, String query){
		if( this.localStoreQueries == null )			
			this.localStoreQueries = new HashMap<String,String>();
		this.localStoreQueries.put(key,query);
		return this;
	}

	public SPARQLFieldMakerStepped addMainStoreQuery(String key, String query){
		if( this.mainStoreQueries == null )
			this.mainStoreQueries = new HashMap<String, String>();
		this.mainStoreQueries.put(key, query);
		return this;		
	}

	public SPARQLFieldMakerStepped  addResultSetToFieldsStepped(ResultSetToFieldsStepped rs2f) {
		if( this.resultSetToFieldsStepped == null )
			this.resultSetToFieldsStepped = new ArrayList<ResultSetToFieldsStepped>();		
		this.resultSetToFieldsStepped.add( rs2f );
		return this;
	}
			
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, RDFService mainStore, RDFService localStore)
			throws Exception {	
		Map<String,ResultSet> resultSet =
				super.runQueries(recordURI, mainStore, localStore, 
						getLocalStoreQueries(), getMainStoreQueries());
		fields.putAll(resultSetsToSolrFields( resultSet ));
		while (( ! mainStoreQueries.isEmpty() ) || ( ! localStoreQueries.isEmpty())) {
			resultSet =	super.runQueries(recordURI, mainStore, localStore, 
					    	getLocalStoreQueries(), getMainStoreQueries());
			fields.putAll(resultSetsToSolrFields( resultSet ));
		}
		return fields;
	}
	
	/**
	 * Convert the result sets generated from running the SPARQL queries to
	 * SolrInputFields. 
	 */
	@Override
	protected Map<? extends String, ? extends SolrInputField> 
		resultSetsToSolrFields( Map<String, ResultSet> results ) 
		throws Exception {
		
		Map<String, SolrInputField> fields = new HashMap<String,SolrInputField>();
		mainStoreQueries = new HashMap<String,String>();
		localStoreQueries = new HashMap<String,String>();
		
		for( ResultSetToFieldsStepped r2f : resultSetToFieldsStepped ){
			
			FieldMakerStep step =  r2f.toFields( results );
			if (step.getFields() != null)
				fields.putAll(step.getFields());
			mainStoreQueries.putAll(step.getMainStoreQueries());			
			localStoreQueries.putAll(step.getLocalStoreQueries());
		}
		return fields;
		
	}		

}
