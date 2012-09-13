package edu.cornell.library.integration.indexer;

import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class IndexingUtilities {
	
	public static String substitueInRecordURI(String recordURI, String query) {
		if( query == null )
			return null;			
		return query.replaceAll("\\$recordURI\\$", "<"+recordURI+">");		
	}

	
	/**
	 * For all newFields, add them to doc, taking into account
	 * fields that already exist in doc and merging the
	 * values of the new and existing fields. 
	 */
	public static void combineFields(SolrInputDocument doc,
			Map<? extends String, ? extends SolrInputField> newFields) {		
		for( String newFieldKey : newFields.keySet() ){
			SolrInputField newField = newFields.get(newFieldKey);
			if( doc.containsKey( newFieldKey )){				
				SolrInputField existingField=doc.get(newFieldKey);
				mergeValuesForField(existingField, newField);
			}else{
				doc.addField(newFieldKey, newField);
			}
		}		
	}

	/**
	 * Call existingField.addValue() for all values form newField.
	 */
	public static void mergeValuesForField(SolrInputField existingField,
			SolrInputField newField) {
		for( Object value  : newField.getValues() ){
			existingField.addValue(value, 1.0f);
		}
	}
}
