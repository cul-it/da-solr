package edu.cornell.library.integration.indexer.documentPostProcess;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Interface to represent objects that will have a 
 * chance to modify a SolrInputDocument once it has
 * been created by the earlier steps in the RecordToDocument.
 */

public interface DocumentPostProcess {
	 /**
	 * @param recordURI
	 * @param document - may be modified by instances of this interface.
	 * @param voyager TODO
	 * @throws Exception 
	 */
	void p( String recordURI, 
			SolrBuildConfig config, 
			SolrInputDocument document) throws Exception;
}
