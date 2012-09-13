package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * This is an example of implementing the ResultSetToFields.
 * 
 */
public class ExampleResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField>fields = new HashMap<String,SolrInputField>();		
				
		//Here is an example of using a result set as a table of lookup values:		
		ResultSet languageNamesRS = results.get("languages");
		
		String fullLanguage = 
			ResultSetUtilities.findValueByKey(languageNamesRS, "ENG");
		
		if( fullLanguage == null ){
			fullLanguage = "unknown";
		}
		
		String fieldName = "FullLanguageName";
		SolrInputField field = new SolrInputField(fieldName);
		field.addValue(fullLanguage,1.0f);
		fields.put(fieldName, field);
		
		
		
		return fields;
	}

}
