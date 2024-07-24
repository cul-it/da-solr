package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
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
	public String getVersion() { return "2.10"; }

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
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) throws SQLException, IOException {
		boolean recordHasFAST = false;
		boolean recordHasLCSH = false;
		final Collection<Heading> taggedFields = new LinkedHashSet<>();
		final Collection<String> subjectDisplay = new LinkedHashSet<>();
		final Collection<String> subjectJson = new LinkedHashSet<>();
		final Collection<String> keywordDisplay = new LinkedHashSet<>();
		final Collection<String> authorityAltForms = new HashSet<>();
		final Collection<String> authorityAltFormsCJK = new HashSet<>();

		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();

		SolrFields sfs = new SolrFields();
		for( DataFieldSet fs: sets ) {

			// For the purposes of id'ing vocabulary, the Roman (last) field is most reliable, so we'll examine that.
			final Heading h = new Heading();
			final DataField f = fs.getFields().get(fs.getFields().size()-1);
			switch (f.ind2) {
			case '0':
				h.vocab = HeadingVocab.LC;
				recordHasLCSH = true;
				break;
			case '1':
				h.vocab = HeadingVocab.LCJSH;
				break;
			case '2':
				h.vocab = HeadingVocab.MESH;
				break;
			case '3':
				h.vocab = HeadingVocab.AGROVOC;
				break;
			case '6':
				h.vocab = HeadingVocab.OTHER;
				break;
			case '7':
				for ( final Subfield sf : f.subfields )
					if (sf.code.equals('2')) {
						switch (sf.value.toLowerCase()) {
						case "fast":
						case "fast/nic":
						case "fast/nic/nac":
							recordHasFAST = true;
							h.vocab = HeadingVocab.FAST;
							break;
						case "lcgft":   h.vocab = HeadingVocab.LCGFT;   break;
						case "lcsh":    h.vocab = HeadingVocab.LC;      break;
						case "aat":     h.vocab = HeadingVocab.AAT;     break;
						case "agrovoc": h.vocab = HeadingVocab.AGROVOC; break;
						case "homoit":  h.vocab = HeadingVocab.HOMOIT;  break;
						case "rbmscv":  h.vocab = HeadingVocab.RBMSCV;  break;
						case "zst":     h.vocab = HeadingVocab.ZST;     break;
						case "local":   h.vocab = HeadingVocab.LOCAL;   break;
						case "ingest":  h.vocab = HeadingVocab.UNK;     break;
						default: h.vocab = HeadingVocab.OTHER;
						}
					}
				break;
			}
			h.fs = fs;
			taggedFields.add(h);
		}
		for( final Heading h : taggedFields) {
			final Set<String> values880_breadcrumbed = new LinkedHashSet<>();
			final Set<String> valuesMain_breadcrumbed = new LinkedHashSet<>();
			final List<BrowseValue> values_browse = new ArrayList<>();
			final Map<HeadingType,String> values_auth_browse = new HashMap<>();
			final Set<String> valuesMain_json = new LinkedHashSet<>();
			final Set<String> values880_json = new LinkedHashSet<>();
			final Set<String> values_dashed = new LinkedHashSet<>();
			HeadingType ht = HeadingType.GENHEAD; //default

			String main_fields = null, dashed_fields = "", facet_type = "topic";
			FieldValues vals = null;
			for (final DataField f: h.fs.getFields()) {

				boolean is880 = f.tag.equals("880");

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
					h.is653 = true;
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
				String origPrimaryTerm = null;
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
					AuthorityData authData = new AuthorityData(config,primarySubjectTerm,ht);
					if ( authData.replacementForm != null ) {
						if ( authData.alternateForms == null ) authData.alternateForms = new ArrayList<>();
						authData.alternateForms.add(primarySubjectTerm);
						origPrimaryTerm = primarySubjectTerm;
						primarySubjectTerm = authData.replacementForm;
					}
					final Breadcrumbs breadcrumbed = new Breadcrumbs( primarySubjectTerm, origPrimaryTerm );
					final List<Object> json = new ArrayList<>();
					final Map<String,Object> subj1 = new HashMap<>();
					subj1.put("subject", primarySubjectTerm);
					subj1.put("type", ht.toString());
					subj1.put("authorized", authData.authorized);
					if (authData.alternateForms != null)
						for (final String altForm : authData.alternateForms) {
							authorityAltForms.add(altForm);
							if (isCJK(altForm))
								authorityAltFormsCJK.add(altForm);
						}
					json.add(subj1);

					values_browse.add(new BrowseValue(breadcrumbed,"\\s?\\(Core\\)$",is880));
					if (vals != null && vals.category.equals(HeadingCategory.AUTHORTITLE) && ! is880) {
						HeadingType nameType = (f.mainTag.equals("600")) ? HeadingType.PERSNAME
								: (f.mainTag.equals("610")) ? HeadingType.CORPNAME : HeadingType.EVENT;
						values_auth_browse.put(nameType, removeTrailingPunctuation(vals.author,"."));
					}
					final List<String> dashed_terms = f.valueListForSpecificSubfields(dashed_fields);
					//					String dashed_terms = f.concatenateSpecificSubfields("|",dashed_fields);
					if (h.is653) {
						sfs.add(new SolrField("sixfivethree",primarySubjectTerm));
					}
					for (final String dashed_term : dashed_terms) {
						final Map<String,Object> subj = new HashMap<>();
						breadcrumbed.append(" > "+dashed_term);
						subj.put("subject", dashed_term);
						values_browse.add(new BrowseValue(breadcrumbed,is880));
						authData = new AuthorityData(config,breadcrumbed.toString(),ht);
						subj.put("authorized", authData.authorized);
						if (authData.authorized && authData.alternateForms != null)
							for (final String altForm : authData.alternateForms) {
								authorityAltForms.add(altForm);
								if (isCJK(altForm))
									authorityAltFormsCJK.add(altForm);
							}
						json.add(subj);
					}
					final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
					mapper.writeValue(jsonstream, json);
					if (is880) {
						values880_breadcrumbed.add(removeTrailingPunctuation(breadcrumbed.toString(),"."));
						values880_json.add(jsonstream.toString("UTF-8"));
					} else {
						valuesMain_breadcrumbed.add(removeTrailingPunctuation(breadcrumbed.toString(),"."));
						valuesMain_json.add(jsonstream.toString("UTF-8"));
					}

					// tabulate subdivision sequences
					if ( ! is880 )
						for ( int i = 0 ; i < dashed_terms.size(); i++ ) {
							String s = dashed_terms.get(i);
							values_dashed.add(s);
							for ( int j = i + 1 ; j < dashed_terms.size(); j++) {
								s += " > "+dashed_terms.get(j);
								values_dashed.add(s);
							}
					}
				}
			}


			for (final String s: values880_breadcrumbed) {
				final String disp = removeTrailingPunctuation(s,".");
				sfs.add(new SolrField("subject_t",s));
				if (h.vocab.equals(HeadingVocab.FAST))
					sfs.add(new SolrField("fast_"+facet_type+"_facet",disp));
				if ( ! h.vocab.equals(HeadingVocab.FAST) || ! recordHasLCSH)
					if (h.is653) keywordDisplay.add(disp.replaceAll("\\s?\\(Core\\)$", ""));
					else         subjectDisplay.add(disp);
			}
			for (final String s: valuesMain_breadcrumbed) {
				String disp = removeTrailingPunctuation(s,".");
				if (facet_type.equals("era"))
					disp = normalizeDateRangeSpacing( disp );
				sfs.add(new SolrField("subject_t",s));
				if (h.vocab.equals(HeadingVocab.FAST)
						|| (h.vocab.equals(HeadingVocab.LCGFT) && facet_type.equals("genre")))
					sfs.add(new SolrField("fast_"+facet_type+"_facet",disp));
				if ( ( ! h.vocab.equals(HeadingVocab.FAST) || ! recordHasLCSH)
					&& ! h.vocab.equals(HeadingVocab.OTHER))
					if (h.is653) keywordDisplay.add(disp.replaceAll("\\s?\\(Core\\)$", ""));
					else         subjectDisplay.add(disp);
			}
			for (final BrowseValue value: values_browse)
				if (ht != null) {
					if (( ! ht.abbrev().equals("topic") || ! unwantedFacetValues.contains(value.display) )
							&& ! h.vocab.equals(HeadingVocab.OTHER) )
						sfs.add(new SolrField("subject_"+ht.abbrev()+"_facet",value.display));
					String filing = getFilingForm(value.display);
					if (! h.vocab.equals(HeadingVocab.OTHER))
						sfs.add(new SolrField("subject_"+ht.abbrev()+"_filing",filing));
					String canonFiling = getFilingForm(value.canon);
					if ( ! value.is880 && ! isCJK(value.canon)) {
						String vocab = h.vocab.name().toLowerCase();
						sfs.add(new SolrField("subject_"+ht.abbrev()+"_"+vocab+"_facet", value.canon));
						sfs.add(new SolrField("subject_"+ht.abbrev()+"_"+vocab+"_filing",canonFiling));
					}
				}
			for (final String s: values_dashed) {
				if ( isCJK(s) ) continue;
				String vocab = h.vocab.name().toLowerCase();
				sfs.add(new SolrField("subject_sub_"+vocab+"_facet", removeTrailingPunctuation(s,".")));
				sfs.add(new SolrField("subject_sub_"+vocab+"_filing",getFilingForm(s)));
			}
			for (final HeadingType parentHt : values_auth_browse.keySet()) {
				String value = values_auth_browse.get(parentHt);
				if (isCJK(value)) continue;
				String vocab = h.vocab.name().toLowerCase();
				sfs.add(new SolrField("subject_"+parentHt.abbrev()+"_"+vocab+"_facet", value));
				sfs.add(new SolrField("subject_"+parentHt.abbrev()+"_"+vocab+"_filing",getFilingForm(value)));
			}

			if ( ! h.is653 &&
					! h.vocab.equals(HeadingVocab.OTHER) &&
					( ! h.vocab.equals(HeadingVocab.FAST) || ! recordHasLCSH ) ) {
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
		for (String display : keywordDisplay)
			sfs.add(new SolrField("keyword_display",display));
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

	private static class Breadcrumbs {
		final StringBuilder display;
		final StringBuilder canon;
		final boolean canonDiffers;

		@Override public String toString() { return this.display.toString(); }

		public void append (String s) {
			this.display.append(s);
			if ( this.canonDiffers ) this.canon.append(s);
		}
		public Breadcrumbs(String display, String canon) {
			this.display = new StringBuilder(display);
			if ( canon == null ) {
				this.canon = this.display;
				this.canonDiffers = false;
			} else {
				this.canon = new StringBuilder( canon );
				this.canonDiffers = true;
			}
		}
	}
	private static class BrowseValue {
		final String display;
		final String canon;
		final boolean is880;
		public BrowseValue( Breadcrumbs bc, String remove, boolean is880 ) {
			this.display = removeTrailingPunctuation(bc.toString(),".").replaceAll(remove, "");
			this.is880 = is880;
			if ( bc.canonDiffers )
				this.canon = removeTrailingPunctuation(bc.canon.toString(),".").replaceAll(remove, "");
			else
				this.canon = this.display;
		}
		public BrowseValue( Breadcrumbs bc, boolean is880 ) {
			this.display = removeTrailingPunctuation(bc.toString(),".");
			this.is880 = is880;
			if ( bc.canonDiffers )
				this.canon = removeTrailingPunctuation(bc.canon.toString(),".");
			else
				this.canon = this.display;
		}
	}
	private static class Heading {
		public Heading() { }
		HeadingVocab vocab = HeadingVocab.UNK;
		boolean is653 = false;
		DataFieldSet fs = null;
	}
	private enum HeadingVocab { LC, LCJSH, LCGFT, FAST, AAT, AGROVOC, HOMOIT, MESH, ZST, RBMSCV, LOCAL, OTHER, UNK; }
}
