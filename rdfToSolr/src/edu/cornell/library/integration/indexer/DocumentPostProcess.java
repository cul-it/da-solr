package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;

public interface DocumentPostProcess {
	void p( String recordURI, 
			RDFQueryService mainStore, 
			RDFQueryService localStore, 
			SolrInputDocument document);
}
