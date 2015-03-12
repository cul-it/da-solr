package edu.cornell.library.integration.indexer.documentPostProcess;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** To boost shadow records, identify them, then set boost to X times current boost.
 *  We're currently boosting the whole record, but we may want to put a special boost
 *  on the title in the future to promote title searches.
 *  */
public class MissingTitleReport implements DocumentPostProcess{

	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {
		
		if (document.getFieldNames().contains("title_display")) {
			SolrInputField titleField = document.getField("title_display");
			if (titleField.getValueCount() == 0) {
				System.out.println("TitleRport: "+recordURI+" has no title.");
			} else if (titleField.getValueCount() > 1) {
				System.out.println("TitleRport: "+recordURI+" has multiple main titles.");
			} else {
				String title = titleField.getValue().toString();
				if (title.length() == 0)
					System.out.println("TitleRport: "+recordURI+" has empty title.");
			}
		} else {
			System.out.println("TitleRport: "+recordURI+" has no title.");
		}
	}

}
