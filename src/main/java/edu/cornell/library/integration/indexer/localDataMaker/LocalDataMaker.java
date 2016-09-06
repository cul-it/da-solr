package edu.cornell.library.integration.indexer.localDataMaker;

import java.io.IOException;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

/**
 * Interface to get data into a local triple store so that
 * queries will not have to go against main triple store.
 * This could also be a step to convert some data into RDF
 * for this record.
 */
public interface LocalDataMaker {
	/** Gather RDF to add to a constructed graph.
	 * This should modify localStore to add any data
	 * that is needed. Ex. run a SPARQL CONSTRUCT against
	 * the mainStoreQueryService and put the results into
	 * localStore. 
	 *  
	 * @throws RDFServiceException */	
	void gather(
			String recordURI, 
			RDFService mainStoreQueryService,
			RDFService localStore) throws RDFServiceException, IOException;  
}
