package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.AuthorityData;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.FieldValues;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * process subject field values into display, facet, search, and browse/filing fields
 *
 */
public class Subject implements SolrFieldGenerator {

	private static ObjectMapper mapper = new ObjectMapper();
	private static List<String> unwantedFacetValues = Arrays.asList("Electronic books");

	@Override
	public String getVersion() { return "1.2"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList("600","610","611","630","648","650","651","653","654","655","656","657","658",
				"662","690","691","692","693","694","695","696","697","698","699");
	}

	@Override
	public boolean providesHeadingBrowseData() { return true; }

	@Override
	// This field generator uses currently untracked authority data, so should be regenerated more often.
	public Duration resultsShelfLife() { return Duration.ofDays(14); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config )
			throws ClassNotFoundException, SQLException, IOException {
		boolean recordHasFAST = false;
		boolean recordHasLCSH = false;
		final Collection<Heading> taggedFields = new LinkedHashSet<>();
		final Collection<String> subjectDisplay = new LinkedHashSet<>();
		final Collection<String> subjectJson = new LinkedHashSet<>();
		final Collection<String> authorityAltForms = new HashSet<>();
		final Collection<String> authorityAltFormsCJK = new HashSet<>();

		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();

		SolrFields sfs = new SolrFields();
		for( DataFieldSet fs: sets ) {

			// First DataField in each FieldSet should be representative, so we'll examine that.
			final Heading h = new Heading();
			final DataField f = fs.getFields().get(0);
			if (f.ind2.equals('7')) {
				for ( final Subfield sf : f.subfields )
					if (sf.code.equals('2')) {
						if (sf.value.equalsIgnoreCase("fast")
								|| sf.value.equalsIgnoreCase("fast/NIC")
								|| sf.value.equalsIgnoreCase("fast/NIC/NAC")) {
							recordHasFAST = true;
							h.isFAST = true;
						} else if (sf.value.equalsIgnoreCase("lcgft")) {
							h.isLCGFT = true;
						}
					}
			} else if (f.ind2.equals('0')) {
				recordHasLCSH = true;
			}
			h.fs = fs;
			taggedFields.add(h);
		}
		for( final Heading h : taggedFields) {
			final Set<String> values880_breadcrumbed = new HashSet<>();
			final Set<String> valuesMain_breadcrumbed = new HashSet<>();
			final Set<String> values_browse = new HashSet<>();
			final Set<String> valuesMain_json = new HashSet<>();
			final Set<String> values880_json = new HashSet<>();
			HeadingType ht = HeadingType.GENHEAD; //default

			String main_fields = null, dashed_fields = "", facet_type = "topic";
			FieldValues vals = null;
			for (final DataField f: h.fs.getFields()) {

				switch (f.mainTag) {
				case "600":
					vals = FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdq;tklnpmors");
					ht = (vals.category.equals(HeadingCategory.AUTHOR)) ?
							HeadingType.PERSNAME : HeadingType.WORK;
					dashed_fields = "vxyz";
					break;
				case "610":
					vals = FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcd;tklnpmors");
					ht = (vals.category.equals(HeadingCategory.AUTHOR)) ?
							HeadingType.CORPNAME : HeadingType.WORK;
					dashed_fields = "vxyz";
					break;
				case "611":
					vals = FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcden;tklpmors");
					ht = (vals.category.equals(HeadingCategory.AUTHOR)) ?
							HeadingType.EVENT : HeadingType.WORK;
					dashed_fields = "vxyz";
					break;
				case "630":
					main_fields = "adfghklmnoprst";
					dashed_fields = "vxyz";
					ht = HeadingType.WORK;
					break;
				case "648":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "era";
					ht = HeadingType.CHRONTERM;
					break;
				case "650":
					main_fields = "abcd";
					dashed_fields = "vxyz";
					ht = HeadingType.TOPIC;
					break;
				case "651":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "geo";
					ht = HeadingType.GEONAME;
					break;
				case "653":
					// This field list is used for subject_display and sixfivethree.
					main_fields = "a";
					break;
				case "654":
					main_fields = "abe";
					dashed_fields = "vyz";
					ht = HeadingType.TOPIC;
					break;
				case "655":
					main_fields = "ab"; //655 facet_type over-ridden for FAST facet
					dashed_fields = "vxyz";
					facet_type = "genre";
					ht = HeadingType.GENRE;
					break;
				case "656":
					main_fields = "ak";
					dashed_fields = "vxyz";
					break;
				case "657":
					main_fields = "a";
					dashed_fields = "vxyz";
					break;
				case "658":
					main_fields = "abcd";
					break;
				case "662":
					main_fields = "abcdfgh";
					facet_type = "geo";
					ht = HeadingType.GEONAME;
					break;
				case "690":
				case "691":
					main_fields = "abvxyz";
					break;
				case "692":
				case "693":
				case "694":
				case "695":
				case "696":
				case "697":
				case "698":
				case "699":
					main_fields = "a";
					break;
				}
				String primarySubjectTerm = null;
				if (vals != null) {
					final StringBuilder sb = new StringBuilder();
					sb.append(vals.author);
					if (vals.category.equals(HeadingCategory.AUTHORTITLE))
						sb.append(" | ").append(vals.title);
					primarySubjectTerm = sb.toString();
				} else if (main_fields != null) {
					primarySubjectTerm = f.concatenateSpecificSubfields(main_fields);
				}
				if (primarySubjectTerm != null) {
					final StringBuilder sb_breadcrumbed = new StringBuilder();
					sb_breadcrumbed.append(primarySubjectTerm);
					final List<Object> json = new ArrayList<>();
					final Map<String,Object> subj1 = new HashMap<>();
					subj1.put("subject", primarySubjectTerm);
					subj1.put("type", ht.toString());
					AuthorityData authData = new AuthorityData(config,primarySubjectTerm,ht);
					subj1.put("authorized", authData.authorized);
					if (authData.authorized && authData.alternateForms != null)
						for (final String altForm : authData.alternateForms) {
							authorityAltForms.add(altForm);
							if (hasCJK(altForm))
								authorityAltFormsCJK.add(altForm);
						}
					json.add(subj1);

					values_browse.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
					final List<String> dashed_terms = f.valueListForSpecificSubfields(dashed_fields);
					//					String dashed_terms = f.concatenateSpecificSubfields("|",dashed_fields);
					if (f.mainTag.equals("653")) {
						sfs.add(new SolrField("sixfivethree",primarySubjectTerm));
					}
					for (final String dashed_term : dashed_terms) {
						final Map<String,Object> subj = new HashMap<>();
						sb_breadcrumbed.append(" > "+dashed_term);
						subj.put("subject", dashed_term);
						values_browse.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
						authData = new AuthorityData(config,sb_breadcrumbed.toString(),ht);
						subj.put("authorized", authData.authorized);
						if (authData.authorized && authData.alternateForms != null)
							for (final String altForm : authData.alternateForms) {
								authorityAltForms.add(altForm);
								if (hasCJK(altForm))
									authorityAltFormsCJK.add(altForm);
							}
						json.add(subj);
					}
					final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
					mapper.writeValue(jsonstream, json);
					if (f.tag.equals("880")) {
						values880_breadcrumbed.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
						values880_json.add(jsonstream.toString("UTF-8"));
					} else {
						valuesMain_breadcrumbed.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
						valuesMain_json.add(jsonstream.toString("UTF-8"));
					}
				}
			}


			for (final String s: values880_breadcrumbed) {
				final String disp = removeTrailingPunctuation(s,".");
				sfs.add(new SolrField("subject_t",s));
				if (h.isFAST)
					sfs.add(new SolrField("fast_"+facet_type+"_facet",disp));
				if ( ! h.isFAST || ! recordHasLCSH)
					subjectDisplay.add(disp);
			}
			for (final String s: valuesMain_breadcrumbed) {
				String disp = removeTrailingPunctuation(s,".");
				if (facet_type.equals("era"))
					disp = normalizeDateRangeSpacing( disp );
				sfs.add(new SolrField("subject_t",s));
				if (h.isFAST || (h.isLCGFT && facet_type.equals("genre")))
					sfs.add(new SolrField("fast_"+facet_type+"_facet",disp));
				if ( ! h.isFAST || ! recordHasLCSH)
					subjectDisplay.add(disp);
			}
			for (final String s: values_browse)
				if (ht != null) {
					if ( ! ht.abbrev().equals("topic") || ! unwantedFacetValues.contains(s) )
						sfs.add(new SolrField("subject_"+ht.abbrev()+"_facet",removeTrailingPunctuation(s,".")));
					sfs.add(new SolrField("subject_"+ht.abbrev()+"_filing",getFilingForm(s)));
				}

			if ( ! h.isFAST || ! recordHasLCSH) {
				for (final String s: values880_json)
					subjectJson.add( s );
				for (final String s: valuesMain_json)
					subjectJson.add( s );
			}
		}

		sfs.add( new BooleanSolrField( "fast_b", recordHasFAST ));

		for (String json : subjectJson)
			sfs.add(new SolrField("subject_json",json));
		for (String display : subjectDisplay)
			sfs.add(new SolrField("subject_display",display));
		for (String altForm : authorityAltForms)
			sfs.add(new SolrField("authority_subject_t",altForm));
		for (String altForm : authorityAltFormsCJK)
			sfs.add(new SolrField("authority_subject_t_cjk",altForm));

		return sfs;
	}

	private static String normalizeDateRangeSpacing(String disp) {
		Matcher m = dateRangePattern.matcher(disp);
		if (m.matches())
			return m.group(1)+" - "+m.group(2);
		return disp;
	}
	private static Pattern dateRangePattern = Pattern.compile("(\\d+)-(\\d+)");

	private static class Heading {
		public Heading() { }
		boolean isFAST = false;
		boolean isLCGFT = false;
		DataFieldSet fs = null;
	}

}
