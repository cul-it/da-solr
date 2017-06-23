package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.Script;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.library.integration.utilities.NameUtils;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class TitleSeries implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("title_series"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {

		SolrFields sfs = new SolrFields();
		TreeSet<DataFieldSet> fss = rec.matchAndSortDataFields();
		List<String> priorityTags = Arrays.asList("830","490","440","400","410","411","800","810","811");
		String displayTag = null;
		for (String possibleTag : priorityTags) {
			for (DataFieldSet fs : fss) {
				if (fs.getMainTag().equals(possibleTag)) {
					displayTag = possibleTag;
					break;
				}
			}
			if (displayTag != null) break;
		}

		for( DataFieldSet fs: fss ) {

			List<FieldValues> ctsValsList = NameUtils.authorAndOrTitleValues(fs);
			for (int i = 0; i < fs.getFields().size(); i++) {
				DataField f = fs.getFields().get(i);
				FieldValues ctsVals = ctsValsList.get(i);
				FieldValues displayVals = FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmorsv");
				
				// TODO 830 initial article work

				// DISPLAY VALUES
				if (f.mainTag.equals(displayTag)) {
					String display = (displayVals.title.equals(HeadType.TITLE)) ? displayVals.title
							: displayVals.author+" | "+displayVals.title;
					sfs.add(new SolrField("title_series_display",display));
					StringBuilder sb = new StringBuilder();
					sb.append(display).append('|').append(ctsVals.title);
					if (ctsVals.type.equals(HeadType.AUTHORTITLE))
						sb.append('|').append(ctsVals.author);
					sfs.add(new SolrField("title_series_cts",sb.toString()));
				}

				// BROWSE ENTRIES
				if (f.mainTag.equals("800") || f.mainTag.equals("810") || f.mainTag.equals("811")) {
					String facetVal = ctsVals.author+" | "+ctsVals.title;
					sfs.add(new SolrField("authortitle_filing",getFilingForm(facetVal)));
					sfs.add(new SolrField("authortitle_facet",
							IndexingUtilities.removeTrailingPunctuation(facetVal,";,. ")));
				}

				// SEARCH FIELDS
				if (f.getScript().equals(Script.CJK)) {
					sfs.add(new SolrField("title_series_t_cjk",displayVals.title));
					if ( displayVals.type.equals(HeadType.AUTHORTITLE) )
						sfs.add(new SolrField("author_addl_t_cjk",displayVals.title));
				} else {
					sfs.add(new SolrField("title_series_t",displayVals.title));
					if ( displayVals.type.equals(HeadType.AUTHORTITLE) )
						sfs.add(new SolrField("author_addl_t",displayVals.title));
				}
			}
		}
		return sfs;
	}
}
