package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

/**
 * Interface to represent objects that will build
 * Solr documents for a recordURI 
 */
public interface RecordToDocument {
	
	/** Query for information about a record and build a Solr document for that record. 
	 * @throws RDFServiceException */
	public 
	SolrInputDocument buildDoc(String recordURI, RDFQueryService queryService ) 
	throws RDFServiceException;

}
