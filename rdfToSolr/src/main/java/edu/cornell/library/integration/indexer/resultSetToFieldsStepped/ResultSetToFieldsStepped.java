package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import java.util.Map;
import com.hp.hpl.jena.query.ResultSet;

/**
 * interface for objects that convert result sets to SolrInputFields.
 */
public interface ResultSetToFieldsStepped {
	
	public 	FieldMakerStep toFields(Map<String, ResultSet> results) 
	throws Exception;
	
}