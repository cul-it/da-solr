package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.AuthorityData;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.indexer.utilities.RelatorSet;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.FieldValues;
import edu.cornell.library.integration.marc.DataField.Script;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * process the whole 7xx range into a wide variety of fields
 *
 */
public class TitleChange implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord();
		rec.addDataFieldResultSet( results.get("added_entry") );
		rec.addDataFieldResultSet( results.get("linking_entry") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, config );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {
		Collection<DataFieldSet> sets = rec.matchAndSortDataFields();

		SolrFields sfs = new SolrFields();
		for( DataFieldSet fs: sets ) {

			if (  Character.getNumericValue(fs.getMainTag().charAt(1)) > 5 )
				sfs.addAll(processLinkingTitleFields(fs));
			else if (  Character.getNumericValue(fs.getMainTag().charAt(1)) <= 1 )
				sfs.addAll(processAuthorAddedEntryFields(config,fs));
			else if ( fs.getMainTag().equals("730") || fs.getMainTag().equals("740"))
				sfs.addAll(processTitleAddedEntryFields(fs));
			else
				System.out.println("Unrecognized field tag: "+fs.getFields().get(0).toString());
		}
		return sfs;
	}

	private static List<SolrField> processTitleAddedEntryFields(DataFieldSet fs) {
		List<SolrField> sfs = new ArrayList<>();
		for (DataField f : fs.getFields()) {
			String workField, title_cts, relation;
			if (f.mainTag.equals("730")) {
				title_cts = f.concatenateSubfieldsOtherThan("6i");
				workField = f.concatenateSpecificSubfields("iaplskfmnordgh");
				final String searchField = f.concatenateSpecificSubfields("abcdefghjklmnopqrstuvwxyz");
				sfs.add(new SolrField("title_uniform_t",searchField));
			} else {
				title_cts = f.concatenateSpecificSubfields("ab");
				workField = f.concatenateSpecificSubfields("iabchqdeklxftgjmnoprsuvwyz");
			}
			if (f.ind2.equals('2'))
				relation = "included_work_display";
			else
				relation = "related_work_display";

			sfs.add(new SolrField( relation, workField+'|'+title_cts));

		}
		return sfs;
	}

	private static List<SolrField> processAuthorAddedEntryFields(SolrBuildConfig config, DataFieldSet fs)
			throws ClassNotFoundException, SQLException, IOException {
		List<FieldValues> ctsValsList  = generateClickToSearchAuthorEntryValues(fs);
		if (ctsValsList == null) return null;

		// Check for special case - exactly two matching AUTHOR entries
		List<DataField> fields = fs.getFields();
		if ( fields.size() == 2
				&& ctsValsList.get(0).type.equals(HeadType.AUTHOR)
				&& ctsValsList.get(0).type.equals(HeadType.AUTHOR)
				&& fields.get(0).mainTag.equals(fields.get(1).mainTag)) {
			return combinedRomanNonRomanAuthorEntry( config, fs, ctsValsList );
		}

		// In all other cases, process the fields in the set individually
		List<SolrField> sfs = new ArrayList<>();
		for ( int i = 0 ; i < fs.getFields().size() ; i++ ) {
			DataField f = fs.getFields().get(i);
			FieldValues ctsVals = ctsValsList.get(i);
			boolean isCJK = f.getScript().equals(Script.CJK);

			if (ctsVals.type.equals(HeadType.AUTHORTITLE)) {
				String browseDisplay = ctsVals.author+" | "+ctsVals.title;
				String relation;
				if (f.ind2.equals('2')) {
					relation = "included_work_display";
					sfs.add(new SolrField("authortitle_facet",getFacetForm(browseDisplay)));
					sfs.add(new SolrField("authortitle_filing",getFilingForm(browseDisplay)));
					sfs.add(new SolrField((isCJK)?"author_addl_t_cjk":"author_addl_t",ctsVals.author));
					String authorFacet = authorEntryFacetValue( f );
					sfs.add(new SolrField("author_facet",getFacetForm(authorFacet)));
					sfs.add(new SolrField(
							(f.mainTag.equals("700"))?"author_pers_filing":
								(f.mainTag.equals("710"))?"author_corp_filing":"author_event_filing",
							getFilingForm(authorFacet)));
				} else {
					relation = "related_work_display";
				}
				FieldValues itemViewDisplay = f.getFieldValuesForNameAndOrTitleField("abcdefghijklmnopqrstuvwxyz");
				sfs.add(new SolrField(relation,
						itemViewDisplay.author+" "+itemViewDisplay.title+"|"+ctsVals.title+"|"+ctsVals.author));
				sfs.add(new SolrField((isCJK)?"title_uniform_t_cjk":"title_uniform_t",ctsVals.title));

			} else {
				String display = authorEntryDisplayValue( f, true );
				String facet = authorEntryFacetValue( f );
				HeadTypeDesc htd;
				String filingField;
				switch (fs.getMainTag()) {
				case "700": htd = HeadTypeDesc.PERSNAME;
				            filingField = "author_pers_filing"; break;
				case "710": htd = HeadTypeDesc.CORPNAME;
				            filingField = "author_corp_filing"; break;
				default:    htd = HeadTypeDesc.EVENT;
				            filingField = "author_event_filing";
				}

				sfs.add(new SolrField( "author_addl_display", display ));
				sfs.add(new SolrField( (isCJK)?"author_addl_t_cjk":"author_addl_t", display ));
				sfs.add(new SolrField( "author_addl_cts", display+'|'+ctsVals.author ));
				sfs.add(new SolrField( "author_facet", getFacetForm( facet )));
				sfs.add(new SolrField( filingField, getFilingForm( facet ) ));

				final Map<String,Object> json = new LinkedHashMap<>();
				json.put("name1", display);
				json.put("search1", ctsVals.author);
				json.put("type", htd.toString());
				final AuthorityData authData = new AuthorityData(config,ctsValsList.get(0).author,htd);
				json.put("authorizedForm", authData.authorized);
				final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, json);
				sfs.add(new SolrField("author_addl_json",jsonstream.toString("UTF-8")));

				if (authData.authorized && authData.alternateForms != null)
					for (final String altForm : authData.alternateForms) {
						sfs.add(new SolrField("authority_author_t",altForm));
						if (isCJK(altForm))
							sfs.add(new SolrField("authority_author_t_cjk",altForm));
					}

			}
		}
		return sfs;

	}

	private static List<SolrField> combinedRomanNonRomanAuthorEntry(
			SolrBuildConfig config, DataFieldSet fs, List<FieldValues> ctsValsList )
					throws SQLException, ClassNotFoundException, IOException {

		String display1 = authorEntryDisplayValue( fs.getFields().get(0), false );
		String display2 = authorEntryDisplayValue( fs.getFields().get(1), true );
		String search1 = authorEntryDisplayValue( fs.getFields().get(0), true );
		String facet1 = authorEntryFacetValue( fs.getFields().get(0) );
		String facet2 = authorEntryFacetValue( fs.getFields().get(1) );
		HeadTypeDesc htd;
		String filingField;
		switch (fs.getMainTag()) {
		case "700": htd = HeadTypeDesc.PERSNAME;
		            filingField = "author_pers_filing"; break;
		case "710": htd = HeadTypeDesc.CORPNAME;
		            filingField = "author_corp_filing"; break;
		default:    htd = HeadTypeDesc.EVENT;
		            filingField = "author_event_filing";
		}

		List<SolrField> sfs = new ArrayList<>();
		sfs.add(new SolrField( "author_addl_display", display1 +" / "+display2 ));
		sfs.add(new SolrField( "author_addl_cts", display1 +'|'+ctsValsList.get(0).author
				+'|'+display2+'|'+ctsValsList.get(1).author ));
		sfs.add(new SolrField( "author_facet", getFacetForm( facet1 )));
		sfs.add(new SolrField( "author_facet", getFacetForm( facet2 )));
		sfs.add(new SolrField( filingField, getFilingForm( facet1 )));
		sfs.add(new SolrField( filingField, getFilingForm( facet2 )));
		if (fs.getFields().get(0).getScript().equals(Script.CJK))
			sfs.add(new SolrField( "author_addl_t_cjk", search1 ));
		else
			sfs.add(new SolrField( "author_addl_t", search1 ));
		sfs.add(new SolrField( "author_addl_t", display2 ));

		final Map<String,Object> json = new LinkedHashMap<>();
		json.put("name1", display1);
		json.put("search1", ctsValsList.get(0).author);
		json.put("name2", display2);
		json.put("search2", ctsValsList.get(1).author);
		json.put("type", htd.toString());
		final AuthorityData authData = new AuthorityData(config,ctsValsList.get(0).author,htd);
		json.put("authorizedForm", authData.authorized);
		final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
		mapper.writeValue(jsonstream, json);
		sfs.add(new SolrField("author_addl_json",jsonstream.toString("UTF-8")));

		if (authData.authorized && authData.alternateForms != null)
			for (final String altForm : authData.alternateForms) {
				sfs.add(new SolrField("authority_author_t",altForm));
				if (isCJK(altForm))
					sfs.add(new SolrField("authority_author_t_cjk",altForm));
			}

		return sfs;
	}

	private static String getFacetForm(String s) {
		return removeTrailingPunctuation(s,",. ");
	}

	private static String authorEntryFacetValue(DataField f) {
		String facetSubfields;
		switch (f.mainTag) {
		case "700": facetSubfields = "abcdq"; break;
		case "710": facetSubfields = "ab";    break;
		case "711": facetSubfields = "abe";   break;
		default:    return null;
		}
		return f.concatenateSpecificSubfields(facetSubfields);
	}

	private static String authorEntryDisplayValue(DataField f, Boolean includeSuffixes) {

		String displaySubfields;
		switch (f.mainTag) {
		case "700": displaySubfields = "abcq"; break;
		case "710":
		case "711": displaySubfields = "abcd";   break;
		default:    return null;
		}
		String mainValue = f.concatenateSpecificSubfields(displaySubfields);
		if ( ! includeSuffixes )
			return removeTrailingPunctuation(mainValue,",. ");
		String suffixes = (f.mainTag.equals("700")) ? f.concatenateSpecificSubfields("d") : "";
		if ( ! suffixes.isEmpty() )
			suffixes = " "+suffixes;
		final RelatorSet rs = new RelatorSet(f);
		if ( ! rs.isEmpty() ) {
			if (suffixes.isEmpty())
				mainValue = RelatorSet.validateForConcatWRelators(mainValue);
			else
				suffixes = RelatorSet.validateForConcatWRelators(suffixes);
			suffixes += ' '+rs.toString();
		}
		if (suffixes.isEmpty())
			if (mainValue.charAt(mainValue.length()-1) == ',')
				mainValue = mainValue.substring(0, mainValue.length()-1);
		return mainValue+suffixes;
	}

	private static List<FieldValues> generateClickToSearchAuthorEntryValues(DataFieldSet fs) {

		String ctsSubfields;
		switch (fs.getMainTag()) {
		case "700": ctsSubfields = "abcdq;tklnpmors"; break;
		case "710":
		case "711": ctsSubfields = "abcd;tklnpmors";   break;
		default:    return null;
		}
		return fs.getFields().stream()
				.map(f -> f.getFieldValuesForNameAndOrTitleField(ctsSubfields))
				.collect(Collectors.toList());
	}

	private static List<SolrField> processLinkingTitleFields(DataFieldSet fs) {

		String relation = null;
		MAIN: switch (fs.getMainTag()) {
		case "780":
			switch (fs.getFields().get(0).ind2) {
			case '0': relation = "continues";			break MAIN;
			case '1': relation = "continues_in_part";	break MAIN;
			case '2':
			case '3': relation = "supersedes";			break MAIN;
			case '4': relation = "merger_of";			break MAIN;
			case '5': relation = "absorbed";			break MAIN;
			case '6': relation = "absorbed_in_part";	break MAIN;
			case '7': relation = "separated_from";		break MAIN;
			default:  return null;
			}
		case "785":
			switch (fs.getFields().get(0).ind2) {
			case '0': relation = "continued_by";		break MAIN;
			case '1': relation = "continued_in_part_by";break MAIN;
			case '2':
			case '3': relation = "superseded_by";		break MAIN;
			case '4': relation = "absorbed_by";			break MAIN;
			case '5': relation = "absorbed_in_part_by";	break MAIN;
			case '6': relation = "split_into";			break MAIN;
			case '7': relation = "merger";				break MAIN;
			default:  return null;
			}
		case "765": relation = "translation_of";	break MAIN;
		case "767": relation = "has_translation";	break MAIN;
		case "775": relation = "other_edition";		break MAIN;
		case "770": relation = "has_supplement";	break MAIN;
		case "772": relation = "supplement_to";		break MAIN;
		case "776": relation = "other_form";		break MAIN;
		case "777": relation = "issued_with";		break MAIN;
		default:    return null;
		}

		List<SolrField> sfs = new ArrayList<>();
		for (DataField f : fs.getFields()) {
			FieldValues authorTitle = f.getFieldValuesForNameAndOrTitleField("abcdgknqrst");
			sfs.add(new SolrField("title_uniform_t",authorTitle.title));
			if (f.tag.equals("880")) {
				if (f.getScript().equals(DataField.Script.CJK))
					sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
				else
					if (hasCJK(authorTitle.title))
						sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
			} else {
				if (isCJK(authorTitle.title))
					sfs.add(new SolrField("title_uniform_t_cjk",authorTitle.title));
			}
			// display no longer dependent on ind1 = '0'.
			StringBuilder sb = new StringBuilder();
			sb.append( f.concatenateSpecificSubfields("i") ).append(' ');
			if (authorTitle.author != null)
				sb.append(authorTitle.author).append(" | ");
			sb.append(authorTitle.title);
			sfs.add(new SolrField(relation+"_display",sb.toString())); // cts to re-add when applicable
		}
		return sfs;
	}
}
