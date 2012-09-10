package edu.cornell.library.integration.indexer;

import java.util.Map;

import org.apache.solr.common.SolrInputField;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;

public interface FieldMaker {
	
	/** For the record identified by recordURI, build the some fields that for
	 * a Solr Document that will be the indexed representation of that record */

	Map<? extends String, ? extends SolrInputField> buildFields( 
			String recordURI, 
			RDFQueryService mainStore, 
			RDFQueryService localStore); 
}
