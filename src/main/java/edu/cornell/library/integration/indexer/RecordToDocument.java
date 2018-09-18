package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

/**
 * Interface to represent objects that will build
 * Solr documents for a recordURI 
 */
public interface RecordToDocument {
	
	/** Query for information about a record and build a Solr document for that record. 
	 * @throws RDFServiceException 
	 * @throws Exception */
	public 	SolrInputDocument buildDoc(String recordURI, Config config ) 
	throws RDFServiceException, Exception;

}
