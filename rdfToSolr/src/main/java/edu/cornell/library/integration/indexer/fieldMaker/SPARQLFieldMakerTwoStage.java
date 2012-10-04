package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

public class SPARQLFieldMakerTwoStage extends SPARQLFieldMakerBase{

	public SPARQLFieldMakerTwoStage setName(String name){
		super.name = name;
		return this;
	}
	
	public SPARQLFieldMakerTwoStage addLocalStoreQuery(String key, String query){
		if( this.localStoreQueries == null )			
			this.localStoreQueries = new HashMap<String,String>();
		
		this.localStoreQueries.put(key,query);
		return this;
	}

	public SPARQLFieldMakerTwoStage addMainStoreQuery(String key, String query){
		if( this.mainStoreQueries == null )
			this.mainStoreQueries = new HashMap<String, String>();
		this.mainStoreQueries.put(key, query);
		return this;		
	}

	public SPARQLFieldMakerTwoStage  addResultSetToFields(ResultSetToFields rs2f) {
		if( this.resultSetToFields == null )
			this.resultSetToFields = new ArrayList<ResultSetToFields>();		
		this.resultSetToFields.add( rs2f );
		return this;
	}
	
	/**
	 * Execute the first stage.  The result of the first stage are the result sets from 
	 * running SPARQL queries.
	 * @throws Exception 
	 */
	protected Map<String, ResultSet> 
	StageOne(String recordURI, RDFService mainStore, RDFService localStore) throws Exception{
		return super.runQueries(recordURI, mainStore, localStore, 
				getLocalStoreQueries(), getMainStoreQueries());
	}

	/**
	 * Execute the second stage. The second stage gets the results from the first stage
	 * and can execute more queries and processing.
	 */
	protected Map<? extends String, ? extends SolrInputField>
	StageTwo(String recordURI, RDFService mainStore, RDFService localStore, Map<String, ResultSet> results){
		return null;
	}
	
	@Override
	protected Map<? extends String, ? extends SolrInputField> resultSetsToSolrFields(
			Map<String, ResultSet> results) throws Exception {
		//not used by this implementation.
		throw new Error("This method is not implemented by this class");
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, RDFService mainStore, RDFService localStore)
			throws Exception {	
		return StageTwo( recordURI, mainStore, localStore, 
				StageOne( recordURI, mainStore, localStore ));
	}
}
