package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.BooleanSolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.AuthorityData;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.FieldValues;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * process subject field values into display, facet, search, and browse/filing fields
 *
 */
public class Subject implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			final Map<String, ResultSet> results, final SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord();
		rec.addDataFieldResultSet( results.get("subjects") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		for ( BooleanSolrField f : vals.boolFields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
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
			final Set<String> values880_piped = new HashSet<>();
			final Set<String> valuesMain_piped = new HashSet<>();
			final Set<String> values880_breadcrumbed = new HashSet<>();
			final Set<String> valuesMain_breadcrumbed = new HashSet<>();
			final Set<String> values_browse = new HashSet<>();
			final Set<String> valuesMain_json = new HashSet<>();
			final Set<String> values880_json = new HashSet<>();
			HeadTypeDesc htd = HeadTypeDesc.GENHEAD; //default

			String main_fields = null, dashed_fields = "", facet_type = "topic";
			FieldValues vals = null;
			for (final DataField f: h.fs.getFields()) {

				switch (f.mainTag) {
				case "600":
					vals = f.getFieldValuesForNameAndOrTitleField("abcdq;tklnpmors");
					htd = (vals.type.equals(HeadType.AUTHOR)) ?
							HeadTypeDesc.PERSNAME : HeadTypeDesc.WORK;
					dashed_fields = "vxyz";
					break;
				case "610":
					vals = f.getFieldValuesForNameAndOrTitleField("abcd;tklnpmors");
					htd = (vals.type.equals(HeadType.AUTHOR)) ?
							HeadTypeDesc.CORPNAME : HeadTypeDesc.WORK;
					dashed_fields = "vxyz";
					break;
				case "611":
					vals = f.getFieldValuesForNameAndOrTitleField("abcden;tklpmors");
					htd = (vals.type.equals(HeadType.AUTHOR)) ?
							HeadTypeDesc.EVENT : HeadTypeDesc.WORK;
					dashed_fields = "vxyz";
					break;
				case "630":
					main_fields = "adfghklmnoprst";
					dashed_fields = "vxyz";
					htd = HeadTypeDesc.WORK;
					break;
				case "648":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "era";
					htd = HeadTypeDesc.CHRONTERM;
					break;
				case "650":
					main_fields = "abcd";
					dashed_fields = "vxyz";
					htd = HeadTypeDesc.TOPIC;
					break;
				case "651":
					main_fields = "a";
					dashed_fields = "vxyz";
					facet_type = "geo";
					htd = HeadTypeDesc.GEONAME;
					break;
				case "653":
					// This field list is used for subject_display and sixfivethree.
					main_fields = "a";
					break;
				case "654":
					main_fields = "abe";
					dashed_fields = "vyz";
					htd = HeadTypeDesc.TOPIC;
					break;
				case "655":
					main_fields = "ab"; //655 facet_type over-ridden for FAST facet
					dashed_fields = "vxyz";
					facet_type = "genre";
					htd = HeadTypeDesc.GENRE;
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
					htd = HeadTypeDesc.GEONAME;
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
					if (vals.type.equals(HeadType.AUTHORTITLE))
						sb.append(" | ").append(vals.title);
					primarySubjectTerm = sb.toString();
				} else if (main_fields != null) {
					primarySubjectTerm = f.concatenateSpecificSubfields(main_fields);
				}
				if (primarySubjectTerm != null) {
					final StringBuilder sb_piped = new StringBuilder();
					final StringBuilder sb_breadcrumbed = new StringBuilder();
					sb_piped.append(primarySubjectTerm);
					sb_breadcrumbed.append(primarySubjectTerm);
					final List<Object> json = new ArrayList<>();
					final Map<String,Object> subj1 = new HashMap<>();
					subj1.put("subject", primarySubjectTerm);
					subj1.put("type", htd.toString());
					AuthorityData authData = new AuthorityData(config,primarySubjectTerm,htd);
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
						sfs.add(new SolrField("sixfivethree",sb_piped.toString()));
					}
					for (final String dashed_term : dashed_terms) {
						final Map<String,Object> subj = new HashMap<>();
						sb_piped.append("|"+dashed_term);
						sb_breadcrumbed.append(" > "+dashed_term);
						subj.put("subject", dashed_term);
						values_browse.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
						authData = new AuthorityData(config,sb_breadcrumbed.toString(),htd);
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
						values880_piped.add(removeTrailingPunctuation(sb_piped.toString(),"."));
						values880_breadcrumbed.add(removeTrailingPunctuation(sb_breadcrumbed.toString(),"."));
						values880_json.add(jsonstream.toString("UTF-8"));
					} else {
						valuesMain_piped.add(removeTrailingPunctuation(sb_piped.toString(),"."));
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
				final String disp = removeTrailingPunctuation(s,".");
				sfs.add(new SolrField("subject_t",s));
				if (h.isFAST || (h.isLCGFT && facet_type.equals("genre")))
					sfs.add(new SolrField("fast_"+facet_type+"_facet",disp));
				if ( ! h.isFAST || ! recordHasLCSH)
					subjectDisplay.add(disp);
			}
			for (final String s: values_browse)
				if (htd != null) {
					sfs.add(new SolrField("subject_"+htd.abbrev()+"_facet",removeTrailingPunctuation(s,".")));
					sfs.add(new SolrField("subject_"+htd.abbrev()+"_filing",getFilingForm(s)));
				}

			if ( ! h.isFAST || ! recordHasLCSH) {
				for (final String s: values880_piped)
					sfs.add(new SolrField("subject_cts",s));
				for (final String s: valuesMain_piped)
					sfs.add(new SolrField("subject_cts",s));
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

	private static class Heading {
		boolean isFAST = false;
		boolean isLCGFT = false;
		DataFieldSet fs = null;
	}

}
