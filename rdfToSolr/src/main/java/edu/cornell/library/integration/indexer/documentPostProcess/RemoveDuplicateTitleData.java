package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 *  Check that title data doesn't contain duplicate translations. This tends to happen in non-Roman script
 *  records, where a main title will include both the original title and an English translation, and the
 *  Romanized field will have the Romanized original title and the same English translation. See
 *  https://issues.library.cornell.edu/browse/DISCOVERYACCESS-657
 */
public class RemoveDuplicateTitleData implements DocumentPostProcess {

	public RemoveDuplicateTitleData() {
		super();
	}

	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {
		
		removeDuplicates(document, "title_vern_display","title_display");
		removeDuplicates(document, "subtitle_vern_display","subtitle_display");
		removeDuplicates(document, "fulltitle_vern_display","fulltitle_display");
		
	}

	private void removeDuplicates( SolrInputDocument document, String vernFieldName, String mainFieldName) {

		
		if ( (! document.containsKey(vernFieldName)) || (! document.containsKey(mainFieldName)))
			return;

		SolrInputField vern_field = document.getField( vernFieldName );
		SolrInputField field = document.getField(mainFieldName);
		
		if ((field.getValueCount() < 1) || (vern_field.getValueCount() < 1))
			return;

		String vernTitle = vern_field.getFirstValue().toString();
		Boolean isRTL = false; //Right to left script
		// If this is a RTL value, the RTL open and close markers will interfere with comparison.
		if (vernTitle.startsWith(RTE_openRTL)) {
			vernTitle = vernTitle.substring(RTE_openRTL.length(), vernTitle.length() - PDF_closeRTL.length());
			isRTL = true;
		}
		String[] mainTitleParts = field.getFirstValue().toString().split(" *= *");
		String[] vernTitleParts = vernTitle.split(" *= *");
		Collection<String> newVernTitleParts = new HashSet<String>();
		if ( (mainTitleParts.length == 1) && (vernTitleParts.length == 1))
			return;

		for (String vernPart : vernTitleParts) {
			Boolean matched = false;
			System.out.println("{"+vernPart+"}");
			for (String mainPart: mainTitleParts) {
				if (vernPart.equals(mainPart)) {
					matched = true;
					System.out.println("\tmatches: {"+mainPart+"}");
				} else {
					System.out.println("\tdoes not match: {"+mainPart+"}");
				}
			}
			if (! matched)
				newVernTitleParts.add(vernPart);
			else
				System.out.println("Dropped title segment: \""+vernPart+"\" as duplicate.");
		}
		System.out.println(newVernTitleParts.toString());
		if (newVernTitleParts.size() < vernTitleParts.length) {
			String newVernTitle = null;
			if (isRTL) {
				newVernTitle = RTE_openRTL + StringUtils.join(newVernTitleParts," = ") + PDF_closeRTL;
			} else {
				newVernTitle = StringUtils.join(newVernTitleParts," = ");
			}
			vern_field.setValue(newVernTitle, 1.0f);
//			document.remove("title_vern_display");
			document.put(vernFieldName, vern_field);
		}

	}
}
