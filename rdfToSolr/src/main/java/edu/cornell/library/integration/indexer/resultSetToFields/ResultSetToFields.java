package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

/**
 * interface for objects that convert result sets to SolrInputFields.
 */
public interface ResultSetToFields {
	
	public 	Map<? extends String, ? extends SolrInputField> 
	toFields(Map<String, ResultSet> results) 
	throws Exception;
	
}
