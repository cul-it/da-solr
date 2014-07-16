package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * If a record isn't marked as an online resource, the any urls in that records that
 * otherwise might be classed as access links, should be "other" links instead.
 */
public class NoAccessUrlsUnlessOnline implements DocumentPostProcess {

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {

		final String accessField = "url_access_display";
		final String otherField = "url_other_display";
		final String onlineField = "online";
		
		if (! document.containsKey(accessField))
			return; // without access links, we have nothing to reclassify in any case
		System.out.println("We have access links.");
		
		if (document.containsKey(onlineField)) {
			SolrInputField field = document.getField( onlineField );
			for( Object o : field.getValues() )
				if ( o.toString().equals("Online") )
					return; // for online resource, we change nothing
		}
		System.out.println("But the resource isn't online!! 🙀");

		SolrInputField combinedLinks;
		if (document.containsKey(otherField)) {
			combinedLinks = document.getField(otherField);
		} else {
			combinedLinks = new SolrInputField(otherField);
		}
		
		SolrInputField accessLinks = document.getField(accessField);
		for ( Object o : accessLinks.getValues() )
			combinedLinks.addValue(o, 1);
		
		document.put(otherField, combinedLinks);
		document.remove(accessField);

	}
}
