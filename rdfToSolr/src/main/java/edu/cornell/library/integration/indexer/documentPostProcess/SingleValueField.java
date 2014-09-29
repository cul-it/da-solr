package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

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
	};

	public SingleValueField(String fieldName, Correction correction) {
		super();
		this.fieldName = fieldName;
		this.correction = correction;
	}

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {
		
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

	private SolrInputField fixConcatenate(SolrInputField field) {
		SolrInputField newField = new SolrInputField( field.getName());
		String str = StringUtils.join(field.getValues(), " ");
		newField.setValue(str, 1.0f);
		return  newField;
	}

	private SolrInputField fixToFirstValue(SolrInputField field) {
		SolrInputField newField = new SolrInputField( field.getName());		
		newField.setValue(field.getFirstValue(), 1.0f);
		return  newField;
	}

}
