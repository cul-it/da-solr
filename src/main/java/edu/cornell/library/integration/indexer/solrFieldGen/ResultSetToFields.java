package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.utilities.Config;

/**
 * interface for objects that convert result sets to SolrInputFields.
 */
public interface ResultSetToFields {
	
	public 	Map<String, SolrInputField> 
	toFields(Map<String, ResultSet> results, Config config) 
	throws Exception;
	
}
