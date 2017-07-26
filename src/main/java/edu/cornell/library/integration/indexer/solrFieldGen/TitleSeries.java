package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.Script;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.IndexingUtilities;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class TitleSeries implements ResultSetToFields, SolrFieldGenerator {

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

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("830","490","440","400","410","411","800","810","811");
	}

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig unused ) {

		SolrFields sfs = new SolrFields();
		List<DataField> fs = rec.matchSortAndFlattenDataFields();
		List<String> priorityTags = Arrays.asList("830","490","440","400","410","411","800","810","811");
		String displayTag = null;
		for (String possibleTag : priorityTags) {
			for (DataField f : fs) {
				if (f.mainTag.equals(possibleTag)) {
					displayTag = possibleTag;
					break;
				}
			}
			if (displayTag != null) break;
		}

		for ( DataField f : fs ) {
			FieldValues ctsVals = enforceTitleOnlyOverAuthorOnly(
					FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmors"));
			FieldValues displayVals = enforceTitleOnlyOverAuthorOnly(
					FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmorsfv"));

			// DISPLAY VALUES
			if (f.mainTag.equals(displayTag)) {
				String display = (displayVals.type.equals(HeadType.TITLE)) ? displayVals.title
						: displayVals.author+" | "+displayVals.title;
				sfs.add(new SolrField("title_series_display",display));

				// cts vals can't currently include pipes between author and title
				StringBuilder sb = new StringBuilder();
				boolean hasAuthor = ctsVals.type.equals(HeadType.AUTHORTITLE);
				if (hasAuthor)
					sb.append(displayVals.author).append(' ');
				sb.append(displayVals.title).append('|').append(ctsVals.title);
				if (hasAuthor)
					sb.append('|').append(ctsVals.author);
				sfs.add(new SolrField("title_series_cts",sb.toString()));
			}

			// BROWSE ENTRIES
			if (ctsVals.type.equals(HeadType.AUTHORTITLE)) {
				String facetVal = ctsVals.author+" | "+ctsVals.title;
				sfs.add(new SolrField("authortitle_filing",getFilingForm(facetVal)));
				sfs.add(new SolrField("authortitle_facet",
						IndexingUtilities.removeTrailingPunctuation(facetVal,";,. ")));
			}

			// SEARCH FIELDS
			String titleWOArticle = f.getStringWithoutInitialArticle(displayVals.title);
			if (f.getScript().equals(Script.CJK)) {
				sfs.add(new SolrField("title_series_t_cjk",displayVals.title));
				if ( displayVals.type.equals(HeadType.AUTHORTITLE) )
					sfs.add(new SolrField("author_addl_t_cjk",displayVals.author));
			} else {
				sfs.add(new SolrField("title_series_t",displayVals.title));
				if ( ! displayVals.title.equals(titleWOArticle) )
					sfs.add(new SolrField("title_series_t",titleWOArticle));
				if ( ! displayVals.title.contains(ctsVals.title) )
					sfs.add(new SolrField("title_series_t",ctsVals.title));
				if ( displayVals.type.equals(HeadType.AUTHORTITLE) )
					sfs.add(new SolrField("author_addl_t",displayVals.author));
			}
		}
		return sfs;
	}

	// If author and no title found, assume bad coding and treat as title only
	private static FieldValues enforceTitleOnlyOverAuthorOnly(FieldValues vals) {
		if ( ! vals.type.equals(HeadType.AUTHOR) )
			return vals;
		vals.type = HeadType.TITLE;
		vals.title = vals.author;
		return vals;
	}
}
