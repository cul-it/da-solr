package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.FilingNormalization.getFilingForm;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.utilities.AuthorityData;
import edu.cornell.library.integration.indexer.utilities.RelatorSet;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadTypeDesc;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.DataField.FieldValues;
import edu.cornell.library.integration.marc.DataField.Script;

public class NameUtils {

	static ObjectMapper mapper = new ObjectMapper();

	public static String facetValue(DataField f) {
		String facetSubfields;
		if (f.mainTag.endsWith("00"))
			facetSubfields = "abcdq";
		else if (f.mainTag.endsWith("10"))
			facetSubfields = "ab";
		else if (f.mainTag.endsWith("11"))
			facetSubfields = "abe";
		else return null;

		return f.concatenateSpecificSubfields(facetSubfields);
	}

	public static String displayValue(DataField f, Boolean includeSuffixes) {

		String displaySubfields;
		if ( f.mainTag.endsWith("00") )
			displaySubfields = "abcq";
		else if ( f.mainTag.endsWith("10") || f.mainTag.endsWith("11") )
			displaySubfields = "abcd";
		else return null;

		String mainValue = f.concatenateSpecificSubfields(displaySubfields);
		if ( ! includeSuffixes )
			return removeTrailingPunctuation(mainValue,",. ");
		String suffixes = (f.mainTag.endsWith("00")) ? f.concatenateSpecificSubfields("d") : "";
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

	public static List<FieldValues> authorAndOrTitleValues(DataFieldSet fs) {

		String ctsSubfields;
		if ( fs.getMainTag().endsWith("00") )
			ctsSubfields = "abcdq;tklnpmors";
		else if ( fs.getMainTag().endsWith("10") || fs.getMainTag().endsWith("11") )
			ctsSubfields = "abcdq;tklnpmors";
		else return null;

		return fs.getFields().stream()
				.map(f -> f.getFieldValuesForNameAndOrTitleField(ctsSubfields))
				.collect(Collectors.toList());
	}

	public static String getFacetForm(String s) {
		return removeTrailingPunctuation(s,",. ");
	}

	public static List<SolrField> combinedRomanNonRomanAuthorEntry(
			SolrBuildConfig config, DataFieldSet fs, List<FieldValues> ctsValsList, Boolean mainAuthor )
					throws SQLException, ClassNotFoundException, IOException {

		String display1 = NameUtils.displayValue( fs.getFields().get(0), false );
		String display2 = NameUtils.displayValue( fs.getFields().get(1), true );
		String search1 = NameUtils.displayValue( fs.getFields().get(0), true );
		String facet1 = NameUtils.facetValue( fs.getFields().get(0) );
		String facet2 = NameUtils.facetValue( fs.getFields().get(1) );
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
		sfs.add(new SolrField( (mainAuthor)?"author_display":"author_addl_display", display1 +" / "+display2 ));
		sfs.add(new SolrField( (mainAuthor)?"author_cts":"author_addl_cts", display1 +'|'+ctsValsList.get(0).author
				+'|'+display2+'|'+ctsValsList.get(1).author ));
		sfs.add(new SolrField( "author_facet", NameUtils.getFacetForm( facet1 )));
		sfs.add(new SolrField( "author_facet", NameUtils.getFacetForm( facet2 )));
		sfs.add(new SolrField( filingField, getFilingForm( facet1 )));
		sfs.add(new SolrField( filingField, getFilingForm( facet2 )));
		if (fs.getFields().get(0).getScript().equals(Script.CJK))
			sfs.add(new SolrField( (mainAuthor)?"author_t_cjk":"author_addl_t_cjk", search1 ));
		else
			sfs.add(new SolrField( (mainAuthor)?"author_t":"author_addl_t", search1 ));
		sfs.add(new SolrField( (mainAuthor)?"author_t":"author_addl_t", display2 ));

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
		sfs.add(new SolrField((mainAuthor)?"author_json":"author_addl_json",jsonstream.toString("UTF-8")));

		if (authData.authorized && authData.alternateForms != null)
			for (final String altForm : authData.alternateForms) {
				sfs.add(new SolrField("authority_author_t",altForm));
				if (isCJK(altForm))
					sfs.add(new SolrField("authority_author_t_cjk",altForm));
			}

		return sfs;
	}

}
