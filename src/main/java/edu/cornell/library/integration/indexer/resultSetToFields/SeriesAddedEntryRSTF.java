package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class SeriesAddedEntryRSTF implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord();
		rec.addDataFieldResultSet(results.get("seriesaddedentry"));
		Set<String> workFacet = new HashSet<String>();
		for( DataField f: rec.data_fields.values() ) {
			String title_cts = f.concatenateSpecificSubfields("tklnpmors");
			String author_cts = null;
			if (f.mainTag.equals("800")) {
				author_cts = f.concatenateSpecificSubfields("abcdq");
			} else if (f.mainTag.equals("810")) {
				author_cts = f.concatenateSpecificSubfields("ab");
			} else {
				author_cts = f.concatenateSpecificSubfields("abe");
			}
			workFacet.add(author_cts+" | "+f.getStringWithoutInitialArticle(title_cts));
		}
		
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		for (String s : workFacet) {
			addField(fields,"authortitle_filing",getFilingForm(s));
			addField(fields,"authortitle_facet",removeTrailingPunctuation(s,";,. "));
		}
		
		return fields;

	}
}
