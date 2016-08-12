package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.FieldValues;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.utilities.AuthorityData;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;

/**
 * process subject field values into display, facet, search, and browse/filing fields
 *
 */
public class SubjectResultSetToFields implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			final Map<String, ResultSet> results, final SolrBuildConfig config) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		final Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		final MarcRecord rec = new MarcRecord();

		for( final String resultKey: results.keySet()){
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		final Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		boolean recordHasFAST = false;
		boolean recordHasLCSH = false;

		final Collection<Heading> taggedFields = new LinkedHashSet<Heading>();
		final Collection<String> authorityAltForms = new HashSet<String>();
		final Collection<String> authorityAltFormsCJK = new HashSet<String>();

		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		final Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( final Integer id: ids) {
			final FieldSet fs = sortedFields.get(id);
			// First DataField in each FieldSet should be representative, so we'll examine that.
			final Heading h = new Heading();
			final DataField f = fs.fields.iterator().next();
			if (f.ind2.equals('7')) {
				for ( final Subfield sf : f.subfields.values() )
					if (sf.code.equals('2')
							&& (sf.value.equalsIgnoreCase("fast")
									|| sf.value.equalsIgnoreCase("fast/NIC")
									|| sf.value.equalsIgnoreCase("fast/NIC/NAC"))) {
						recordHasFAST = true;
						h.isFAST = true;

					}
			} else if (f.ind2.equals('0')) {
				recordHasLCSH = true;
			}
			h.fs = fs;
			taggedFields.add(h);
		}
		for( final Heading h : taggedFields) {
			final DataField[] dataFields = h.fs.fields.toArray( new DataField[ h.fs.fields.size() ]);
			final Set<String> values880_piped = new HashSet<String>();
			final Set<String> valuesMain_piped = new HashSet<String>();
			final Set<String> values880_breadcrumbed = new HashSet<String>();
			final Set<String> valuesMain_breadcrumbed = new HashSet<String>();
			final Set<String> values_browse = new HashSet<String>();
			final Set<String> valuesMain_json = new HashSet<String>();
			final Set<String> values880_json = new HashSet<String>();
			HeadTypeDesc htd = HeadTypeDesc.GENHEAD; //default

			String main_fields = null, dashed_fields = "", facet_type = "topic";
			FieldValues vals = null;
			for (final DataField f: dataFields) {

				switch (f.mainTag) {
				case "600":
					vals = f.getFieldValuesForNameMaybeTitleField("abcdq;tklnpmors");
					htd = (vals.type.equals(HeadType.AUTHOR)) ?
							HeadTypeDesc.PERSNAME : HeadTypeDesc.WORK;
					dashed_fields = "vxyz";
					break;
				case "610":
					vals = f.getFieldValuesForNameMaybeTitleField("ab;tklnpmors");
					htd = (vals.type.equals(HeadType.AUTHOR)) ?
							HeadTypeDesc.CORPNAME : HeadTypeDesc.WORK;
					dashed_fields = "vxyz";
					break;
				case "611":
					vals = f.getFieldValuesForNameMaybeTitleField("abcden;tklpmors");
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
				if (main_fields != null && ! main_fields.equals("")) {
					final StringBuilder sb_piped = new StringBuilder();
					final StringBuilder sb_breadcrumbed = new StringBuilder();
					String mainFields = null;
					if (vals != null) {
						final StringBuilder sb = new StringBuilder();
						sb.append(vals.author);
						if (vals.type.equals(HeadType.AUTHORTITLE))
							sb.append(" | ").append(vals.title);
						mainFields = sb.toString();
					} else {
						mainFields = f.concatenateSpecificSubfields(main_fields);
					}
					sb_piped.append(mainFields);
					sb_breadcrumbed.append(mainFields);
					final List<Object> json = new ArrayList<Object>();
					final Map<String,Object> subj1 = new HashMap<String,Object>();
					subj1.put("subject", mainFields);
					subj1.put("type", htd.toString());
					AuthorityData authData = new AuthorityData(config,mainFields,htd);
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
						addField(solrFields,"sixfivethree",sb_piped.toString());
					}
					for (final String dashed_term : dashed_terms) {
						final Map<String,Object> subj = new HashMap<String,Object>();
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
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					addField(solrFields,"fast_"+facet_type+"_facet",disp);
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_display",disp);
			}
			for (final String s: valuesMain_breadcrumbed) {
				final String disp = removeTrailingPunctuation(s,".");
				addField(solrFields,"subject_addl_t",s);
				if (h.isFAST)
					addField(solrFields,"fast_"+facet_type+"_facet",disp);
				if ( ! h.isFAST || ! recordHasLCSH)
					addField(solrFields,"subject_display",disp);
			}
			for (final String s: values_browse)
				if (htd != null) {
					addField(solrFields,"subject_"+htd.abbrev()+"_facet",removeTrailingPunctuation(s,"."));
					addField(solrFields,"subject_"+htd.abbrev()+"_filing",getFilingForm(s));
				}

			if ( ! h.isFAST || ! recordHasLCSH) {
				for (final String s: values880_piped)
					addField(solrFields,"subject_cts",s);
				for (final String s: valuesMain_piped)
					addField(solrFields,"subject_cts",s);
				for (final String s: values880_json)
					addField(solrFields,"subject_json",s);
				for (final String s: valuesMain_json)
					addField(solrFields,"subject_json",s);
			}
		}

		final SolrInputField field = new SolrInputField("fast_b");
		field.setValue(recordHasFAST, 1.0f);
		solrFields.put("fast_b", field);

		for (String altForm : authorityAltForms)
			addField(solrFields,"authority_subject_t",altForm);
		for (String altForm : authorityAltFormsCJK)
			addField(solrFields,"authority_subject_t_cjk",altForm);

		return solrFields;
	}

	private class Heading {
		boolean isFAST = false;
		FieldSet fs = null;
	}

}
