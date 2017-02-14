package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

public class FieldMakerStep {
	
	Map<String,SolrInputField> fields;

	/** Gets the SPARQL queries to run.  These get run against
	 * the local store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	Map<String,String> localStoreQueries = new HashMap<>();
	
	/** Gets the SPARQL queries to run. These get run against the
	 * main store. This should return a 
	 * Map of name_for_query -> SPARQL_query */
	Map<String,String> mainStoreQueries = new HashMap<>();

	

	public Map<String, SolrInputField> getFields() {
		return fields;
	}
	public void setFields(Map<String, SolrInputField> fields) {
		this.fields = fields;		
	}
	public void addField(String name, SolrInputField field) {
		fields.put(name, field);
	}
	
	public void addMainStoreQuery( String name, String query ) {
		if( mainStoreQueries == null )			
			mainStoreQueries = new HashMap<>();		
		mainStoreQueries.put(name, query);
	}
	public void addLocalStoreQuery( String name, String query ) {
		if( localStoreQueries == null )			
			localStoreQueries = new HashMap<>();		
		localStoreQueries.put(name, query);
	}
	
	public Map<String,String> getMainStoreQueries() {
		return mainStoreQueries;
	}
	public Map<String,String> getLocalStoreQueries() {
		return localStoreQueries;
	}
}
