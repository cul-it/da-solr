package edu.cornell.library.integration.indexer.localDataMaker;

import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;

/**
 * Interface to get data into a local triple store so that
 * queries will not have to go against main triple store.
 * This could also be a step to convert some data into RDF
 * for this record.
 */
public interface LocalDataMaker {
	/** Gather RDF to add to a constructed graph. */
	ChangeSet gather( String recordURI ,RDFQueryService mainStore );  
}
