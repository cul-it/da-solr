package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;

import java.util.ArrayList;
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
	public String getVersion() { return "1.1"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("830","490","440","400","410","411","800","810","811");
	}

	@Override
	public boolean providesHeadingBrowseData() { return true; }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		List<String> display490s = new ArrayList<>();
		List<String> cts490s = new ArrayList<>();
		boolean non490 = false;
		SolrFields sfs = new SolrFields();
		List<DataField> fs = rec.matchSortAndFlattenDataFields();

		for ( DataField f : fs ) {
			FieldValues ctsVals = enforceTitleOnlyOverAuthorOnly(
					FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmors"));
			FieldValues displayVals = enforceTitleOnlyOverAuthorOnly(
					FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmorsfv"));

			// DISPLAY VALUES
			String display = (displayVals.category.equals(HeadingCategory.TITLE)) ? displayVals.title
					: displayVals.author+" | "+displayVals.title;
			if ( f.mainTag.equals("490") )
				display490s.add(display);
			else {
				sfs.add(new SolrField("title_series_display",display));
				non490 = true;
			}

			// cts vals can't currently include pipes between author and title
			StringBuilder sb = new StringBuilder();
			boolean hasAuthor = ctsVals.category.equals(HeadingCategory.AUTHORTITLE);
			if (hasAuthor)
				sb.append(displayVals.author).append(' ');
			sb.append(displayVals.title).append('|').append(ctsVals.title);
			if (hasAuthor)
				sb.append('|').append(ctsVals.author);
			if ( f.mainTag.equals("490") ) cts490s.add(sb.toString());
			else                           sfs.add(new SolrField("title_series_cts",sb.toString()));

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
		if (non490 == false) {
			for (String d: display490s) sfs.add(new SolrField("title_series_display",d));
			for (String cts: cts490s)   sfs.add(new SolrField("title_series_cts",cts));
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
