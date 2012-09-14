package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.Map;

import org.apache.solr.common.SolrInputField;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

public interface FieldMaker {
	public String getName();
	
	/** 	
	 * For the record identified by recordURI, build the some fields that for
	 * a Solr Document that will be the indexed representation of that record  
	 * @param recordURI - URI of record to build Solr document for.
	 * @param mainStore - Service for main triple store
	 * @param localStore - Service for localy constructed triple store
	 * @return A Map of Solr field name to SolrInputField objects.
	 * @throws Exception 
	 */
	Map<? extends String, ? extends SolrInputField> buildFields( 
			String recordURI, 
			RDFService mainStore, 
			RDFService localStore) throws Exception; 
}
