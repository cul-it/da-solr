package edu.cornell.library.integration.indexer.documentPostProcess;

import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Enforce that a field is not multi-valued.
 *  
 */
public class SingleValueField implements DocumentPostProcess {

	String fieldName;
	Correction correction;
	
	
	public enum Correction { 
			firstValue, //use the first value 
			concatenate, // concatenate the values together with spaces between
			throwException // throw an exception
	}

	public SingleValueField(String fieldName, Correction correction) {
		super();
		this.fieldName = fieldName;
		this.correction = correction;
	}

	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {
		
		if (! document.containsKey(fieldName)) {
			return;
		}
		SolrInputField field = document.getField( fieldName );
		
		if( fieldName != null && field.getValueCount() > 1 ){		
			switch( correction){
				case firstValue:					
					document.put(fieldName, fixToFirstValue( field ));
					break;
				case concatenate:
					document.put(fieldName, fixConcatenate( field ));
					break;
				case throwException:
					throw new Exception("Field " + fieldName + " has multiple values.\n" 
							+ field.toString());					
			}
		}		
	}

	private static SolrInputField fixConcatenate(SolrInputField field) {
		SolrInputField newField = new SolrInputField( field.getName());
		String str = String.join(" ",field.getValues().stream().map(Object::toString).collect(Collectors.toList()));
		newField.setValue(str, 1.0f);
		return  newField;
	}

	private static SolrInputField fixToFirstValue(SolrInputField field) {
		SolrInputField newField = new SolrInputField( field.getName());		
		newField.setValue(field.getFirstValue(), 1.0f);
		return  newField;
	}

}
