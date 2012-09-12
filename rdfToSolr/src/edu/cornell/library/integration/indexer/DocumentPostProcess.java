package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * Interface to represent objects that will have a 
 * chance to modify a SolrInputDocument once it has
 * been created by the earlier steps in the RecordToDocument.
 */

public interface DocumentPostProcess {
	 /**
	 * @param recordURI
	 * @param mainStore
	 * @param localStore
	 * @param document - may be modified by instances of this interface.
	 */
	void p( String recordURI, 
			RDFService mainStore, 
			RDFService localStore, 
			SolrInputDocument document);
}
