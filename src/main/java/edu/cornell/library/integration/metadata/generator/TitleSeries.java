package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.Script;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class TitleSeries implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("830","490","440","400","410","411","800","810","811");
	}

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

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
				String display = (displayVals.category.equals(HeadingCategory.TITLE)) ? displayVals.title
						: displayVals.author+" | "+displayVals.title;
				sfs.add(new SolrField("title_series_display",display));

				// cts vals can't currently include pipes between author and title
				StringBuilder sb = new StringBuilder();
				boolean hasAuthor = ctsVals.category.equals(HeadingCategory.AUTHORTITLE);
				if (hasAuthor)
					sb.append(displayVals.author).append(' ');
				sb.append(displayVals.title).append('|').append(ctsVals.title);
				if (hasAuthor)
					sb.append('|').append(ctsVals.author);
				sfs.add(new SolrField("title_series_cts",sb.toString()));
			}

			// BROWSE ENTRIES
			if (ctsVals.category.equals(HeadingCategory.AUTHORTITLE)) {
				String facetVal = ctsVals.author+" | "+ctsVals.title;
				sfs.add(new SolrField("authortitle_filing",getFilingForm(facetVal)));
				sfs.add(new SolrField("authortitle_facet",
						IndexingUtilities.removeTrailingPunctuation(facetVal,";,. ")));
			}

			// SEARCH FIELDS
			String titleWOArticle = f.getStringWithoutInitialArticle(displayVals.title);
			if (f.getScript().equals(Script.CJK)) {
				sfs.add(new SolrField("title_series_t_cjk",displayVals.title));
				if ( displayVals.category.equals(HeadingCategory.AUTHORTITLE) )
					sfs.add(new SolrField("author_addl_t_cjk",displayVals.author));
			} else {
				sfs.add(new SolrField("title_series_t",displayVals.title));
				if ( ! displayVals.title.equals(titleWOArticle) )
					sfs.add(new SolrField("title_series_t",titleWOArticle));
				if ( ! displayVals.title.contains(ctsVals.title) )
					sfs.add(new SolrField("title_series_t",ctsVals.title));
				if ( displayVals.category.equals(HeadingCategory.AUTHORTITLE) )
					sfs.add(new SolrField("author_addl_t",displayVals.author));
			}
		}
		return sfs;
	}

	// If author and no title found, assume bad coding and treat as title only
	private static FieldValues enforceTitleOnlyOverAuthorOnly(FieldValues vals) {
		if ( ! vals.category.equals(HeadingCategory.AUTHOR) )
			return vals;
		vals.category = HeadingCategory.TITLE;
		vals.title = vals.author;
		return vals;
	}
}
