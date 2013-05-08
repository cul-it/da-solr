package edu.cornell.library.integration.indexer.documentPostProcess;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.indexer.fieldMaker.SubfieldCodeMaker;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/** To boost shadow records, identify them, then set boost to X times current boost.
 *  We're currently boosting the whole record, but we may want to put a special boost
 *  on the title in the future to promote title searches.
 *  */
public class RecordBoost implements DocumentPostProcess{

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document) throws Exception {
		
		Boolean isShadow = false;
		SubfieldCodeMaker scm = new SubfieldCodeMaker("shadow_flag","948","h");
		Map<? extends String, ? extends SolrInputField> tempfields = 
				scm.buildFields(recordURI, mainStore, localStore);
		if (! tempfields.containsKey("shadow_flag")) return;
		SolrInputField shadowflagfield = tempfields.get("shadow_flag");
		if (shadowflagfield.getValueCount() == 0) return;
		Iterator<Object> i = shadowflagfield.getValues().iterator();
		while (i.hasNext()) {
			String val = i.next().toString();
			if (val.contains("PUBLIC SERVICES SHADOW RECORD")) {
				isShadow = true;
			}
		}

		if (isShadow) {
	//		System.out.println("It's a shadow.");
			document.setDocumentBoost(50 * document.getDocumentBoost());

		}
	}

}
