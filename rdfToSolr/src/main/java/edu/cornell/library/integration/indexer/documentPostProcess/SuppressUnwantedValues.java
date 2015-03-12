package edu.cornell.library.integration.indexer.documentPostProcess;

import java.util.Iterator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** When field values are being generated in multiple places and/or with standard field
 *  building tools, it may be easier to remove values that should be suppressed during
 *  post-process.
 *  */
public class SuppressUnwantedValues implements DocumentPostProcess{

	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		String fieldName = "subject_topic_facet";
		String value = "Electronic books";
		
		eliminateFieldValue(document,fieldName,value);
		
	}

	/* Eliminate value from field fieldName, if it appears. */
	public void eliminateFieldValue(SolrInputDocument document, String fieldName, String value) {
		if (! document.containsKey(fieldName)) {
			return;
		}
		SolrInputField field = document.getField( fieldName );
		if( field.getValueCount() > 1 ){
			SolrInputField newField = new SolrInputField(fieldName);
			Iterator<Object> i = field.getValues().iterator();
			while (i.hasNext()) {
				String val = i.next().toString();
				if (! val.equals(value)) {
					newField.addValue(val, 1.0f);
				}
			}
			document.put(fieldName, newField);
		}
		
	}
	
}
