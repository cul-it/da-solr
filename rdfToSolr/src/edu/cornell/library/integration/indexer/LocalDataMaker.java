package edu.cornell.library.integration.indexer;

import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeSet;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFQueryService;


public interface LocalDataMaker {
	/** Gather RDF to add to a constructed graph. */
	ChangeSet gather( String recordURI ,RDFQueryService mainStore );  
}
