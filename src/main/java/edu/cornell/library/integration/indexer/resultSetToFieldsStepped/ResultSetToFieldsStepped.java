package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import java.util.Map;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * interface for objects that convert result sets to SolrInputFields.
 */
public interface ResultSetToFieldsStepped {
	
	public 	FieldMakerStep toFields(Map<String, ResultSet> results, SolrBuildConfig config) 
	throws Exception;
	
}