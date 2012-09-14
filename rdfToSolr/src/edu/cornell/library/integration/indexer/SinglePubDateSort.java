package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/** need a single pub_date_sort, 
 * plan:
 * remove copy field from schemal.xml
 * copy pub_date to pub_date_sort
 * remove any pub_date_sort values beyond first
 *  */
public class SinglePubDateSort implements DocumentPostProcess{

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document) {

		SolrInputField pubDateSort = document.getField("pub_date_sort");
		SolrInputField pubDate = document.getField("pub_date");
		
		//do our own fake copy field
		if( pubDateSort == null && pubDate != null ){
			document.addField("pub_date_sort", pubDate.getFirstValue());			
		}else{
			if( pubDateSort.getValueCount() > 1 ){
				Object value1 = pubDateSort.getFirstValue();
				pubDateSort = new SolrInputField("pub_date_sort");
				pubDateSort.setValue(value1, 1.0f);
				document.put( "pub_date_sort", pubDateSort);
			}
		}
		
		
		
	}

}
