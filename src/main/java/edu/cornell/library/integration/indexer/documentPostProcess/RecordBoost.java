package edu.cornell.library.integration.indexer.documentPostProcess;

import java.util.Iterator;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** To boost shadow records, identify them, then set boost to X times current boost.
 *  We're currently boosting the whole record, but we may want to put a special boost
 *  on the title in the future to promote title searches.
 *  */
public class RecordBoost implements DocumentPostProcess{

	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {
		
		int boost = 1;
		if (document.getFieldNames().contains("boost")) {
			SolrInputField field = document.getField("boost");
			if (field.getValueCount() > 0) {
				Iterator<Object> i = field.getValues().iterator();
				while (i.hasNext()) {
					String val = i.next().toString();
					if (val.equals("shadowLink")) 
						boost *= 50;
				}
			}
			document.removeField("boost");
		}
		if (boost != 1) {
			document.setDocumentBoost(boost * document.getDocumentBoost());

		}
	}

}
