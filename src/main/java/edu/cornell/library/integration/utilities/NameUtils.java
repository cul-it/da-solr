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

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataField.Script;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.metadata.support.AuthorityData;
import edu.cornell.library.integration.metadata.support.HeadingCategory;
import edu.cornell.library.integration.metadata.support.HeadingType;
import edu.cornell.library.integration.metadata.support.RelatorSet;
import edu.cornell.library.integration.metadata.support.RelatorSet.ProvStatus;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

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
		else {
			facetSubfields = "abcdq";
		}

		return f.concatenateSpecificSubfields(facetSubfields);
	}

	public static String displayValue(DataField f, RelatorSet rs, Boolean includeSuffixes) {

		String displaySubfields;
		if ( f.mainTag.endsWith("00") || f.mainTag.endsWith("20") )
			displaySubfields = "abcq";
		else if ( f.mainTag.endsWith("10") || f.mainTag.endsWith("11") )
			displaySubfields = "abcdn";
		else return null;

		String mainValue = f.concatenateSpecificSubfields(displaySubfields);
		if (mainValue.isEmpty())
			return null;

		if ( ! includeSuffixes )
			return removeTrailingPunctuation(mainValue,",. ");
		String suffixes = (f.mainTag.endsWith("00")) ? f.concatenateSpecificSubfields("d") : "";
		if ( ! suffixes.isEmpty() )
			suffixes = " "+suffixes;
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
		if ( fs.getMainTag().endsWith("00") || fs.getMainTag().endsWith("20"))
			ctsSubfields = "abcdq;tklnpmors";
		else if ( fs.getMainTag().endsWith("10") || fs.getMainTag().endsWith("11") )
			ctsSubfields = "abcdq;tklnpmors";
		else return null;

		return fs.getFields().stream()
				.map(f -> FieldValues.getFieldValuesForNameAndOrTitleField(f,ctsSubfields))
				.collect(Collectors.toList());
	}

	public static FieldValues authorAndOrTitleValues(DataField f) {

		String ctsSubfields;
		if ( f.mainTag.endsWith("00") || f.mainTag.endsWith("20"))
			ctsSubfields = "abcdq;tklnpmors";
		else if ( f.mainTag.endsWith("10") || f.mainTag.endsWith("11") )
			ctsSubfields = "abcdq;tklnpmors";
		else return null;

		return FieldValues.getFieldValuesForNameAndOrTitleField(f,ctsSubfields);

	}

	public static String getFacetForm(String s) {
		return removeTrailingPunctuation(s,",. ");
	}

	public static List<SolrField> combinedRomanNonRomanAuthorEntry(
			Config config, DataFieldSet fs, List<FieldValues> ctsValsList, Boolean isMainAuthor )
					throws SQLException, IOException {

		RelatorSet rs = new RelatorSet(fs.getFields().get(1));

		String display1 = NameUtils.displayValue( fs.getFields().get(0), null, false );
		String display2 = NameUtils.displayValue( fs.getFields().get(1), rs,   true );
		String search1 = NameUtils.displayValue(  fs.getFields().get(0), rs,   true );
		String facet1 = NameUtils.facetValue( fs.getFields().get(0) );
		String facet2 = NameUtils.facetValue( fs.getFields().get(1) );
		HeadingType ht;
		String filingField;
		String romanFilingField;
		if ( fs.getMainTag().endsWith("00")) {
			ht = HeadingType.PERSNAME;
			filingField = "author_pers_filing";
			romanFilingField = "author_pers_roman_filing";
		} else if ( fs.getMainTag().endsWith("10") ) {
			ht = HeadingType.CORPNAME;
			filingField = "author_corp_filing";
			romanFilingField = "author_corp_roman_filing";
		} else if ( fs.getMainTag().endsWith("11") ) {
			ht = HeadingType.EVENT;
			filingField = "author_event_filing";
			romanFilingField = "author_event_roman_filing";
		} else {
			ht = null; filingField = null; romanFilingField = null;
		}

		List<SolrField> sfs = new ArrayList<>();
		// romanFilingField is a staff-facing search endpoint for authority control maintenance.
		String romanFiling = getFilingForm( facet2 );
		if ( romanFilingField != null && ! isCJK(facet2) )
			sfs.add(new SolrField( romanFilingField, romanFiling ));

		boolean includeInAuthor = ! rs.isProvenance().equals(ProvStatus.PROV_ONLY);
		boolean includeInProvenance = ! rs.isProvenance().equals(ProvStatus.NONE);
		boolean displayAsFMO = rs.isProvenance().equals(ProvStatus.PROV_ONLY);

		if (includeInProvenance) {
			rs.remove("former owner");
			String d = NameUtils.displayValue( fs.getFields().get(1), rs, true );
			if (displayAsFMO)
				sfs.add(new SolrField( "former_owner_display" , display1 +" / "+d));
			sfs.add(new SolrField( "former_owner_t", display1 ));
			sfs.add(new SolrField( "former_owner_t", d ));
		}


		if ( ! includeInAuthor) return sfs;

		sfs.add(new SolrField( (isMainAuthor)? "author_display":"author_addl_display", display1 +" / "+display2));
		sfs.add(new SolrField( "author_facet", NameUtils.getFacetForm( facet1 )));
		sfs.add(new SolrField( "author_facet", NameUtils.getFacetForm( facet2 )));
		if (filingField != null) {
			sfs.add(new SolrField( filingField, getFilingForm( facet1 )));
			sfs.add(new SolrField( filingField, romanFiling ));
		}
		if (fs.getFields().get(0).getScript().equals(Script.CJK))
			sfs.add(new SolrField( (isMainAuthor)?"author_t_cjk":"author_addl_t_cjk", search1 ));
		else
			sfs.add(new SolrField( (isMainAuthor)?"author_t":"author_addl_t", search1 ));
		sfs.add(new SolrField( (isMainAuthor)?"author_t":"author_addl_t", display2 ));

		final Map<String,Object> json = new LinkedHashMap<>();
		json.put("name1", display1);
		json.put("search1", ctsValsList.get(0).author);
		json.put("name2", display2);
		json.put("search2", ctsValsList.get(1).author);
		json.put("relator", (new RelatorSet(fs.getFields().get(0))).toString() );
		if (ht != null) json.put("type", ht.toString());
		final AuthorityData authData = (ht != null) 
				? new AuthorityData(config,ctsValsList.get(ctsValsList.size()-1).author,ht)
						: new AuthorityData(false);
		json.put("authorizedForm", authData.authorized);
		final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
		mapper.writeValue(jsonstream, json);
		sfs.add(new SolrField((isMainAuthor)?"author_json":"author_addl_json",jsonstream.toString("UTF-8")));

		if (authData.authorized && authData.alternateForms != null)
			for (final String altForm : authData.alternateForms) {
				sfs.add(new SolrField("authority_author_t",altForm));
				if (isCJK(altForm))
					sfs.add(new SolrField("authority_author_t_cjk",altForm));
			}

		return sfs;
	}

	public static List<SolrField> singleAuthorEntry(
			Config config, DataField f, FieldValues ctsVals, Boolean isMainAuthor)
					throws SQLException, IOException {
		boolean isCJK = f.getScript().equals(Script.CJK);

		List<SolrField> sfs = new ArrayList<>();
		if ( ! isMainAuthor && ctsVals.category.equals(HeadingCategory.AUTHORTITLE)) {
			String browseDisplay = ctsVals.author+" | "+ctsVals.title;
			String relation;

			// ONE FIELD; INCLUDED WORK 7XX
			if (f.ind2.equals('2')) {
				relation = "included_work_display";
				sfs.add(new SolrField("authortitle_facet",NameUtils.getFacetForm(browseDisplay)));
				sfs.add(new SolrField("authortitle_filing",getFilingForm(browseDisplay)));
				sfs.add(new SolrField((isCJK)?"author_addl_t_cjk":"author_addl_t",ctsVals.author));
				String authorFacet = NameUtils.facetValue( f );
				sfs.add(new SolrField("author_facet",NameUtils.getFacetForm(authorFacet)));
				String filingAuthorName = getFilingForm(authorFacet);
				if ( f.mainTag.equals("700") ) {
					sfs.add(new SolrField("author_pers_filing",filingAuthorName));
					if ( ! f.tag.equals("880") && ! isCJK( authorFacet ) )
						sfs.add(new SolrField("author_pers_roman_filing",filingAuthorName));
				} else if ( f.mainTag.equals("710") ) {
					sfs.add(new SolrField("author_corp_filing",filingAuthorName));
					if ( ! f.tag.equals("880") && ! isCJK( authorFacet ) )
						sfs.add(new SolrField("author_corp_roman_filing",filingAuthorName));
				} else {
					sfs.add(new SolrField("author_event_filing",filingAuthorName));
					if ( ! f.tag.equals("880") && ! isCJK( authorFacet ) )
						sfs.add(new SolrField("author_event_roman_filing",filingAuthorName));
				}
			} else {
				relation = "related_work_display";
			}
			FieldValues itemViewDisplay =
					FieldValues.getFieldValuesForNameAndOrTitleField(f,"abcdefghijklmnopqrstuvwxyz");
			sfs.add(new SolrField(relation,itemViewDisplay.author+" "
					+itemViewDisplay.title+"|"+ctsVals.title+"|"+ctsVals.author));
			sfs.add(new SolrField((isCJK)?"title_uniform_t_cjk":"title_uniform_t",ctsVals.title));

		// ONE FIELD; JUST AUTHOR
		} else {
			RelatorSet rs = new RelatorSet(f);

			String display = NameUtils.displayValue( f, rs, true );
			if (display == null)
				return sfs;
			String facet = NameUtils.facetValue( f );
			String filing = getFilingForm( facet );

			HeadingType ht;
			String filingField;
			String romanFilingField;
			if (f.mainTag.endsWith("00")) {
				ht = HeadingType.PERSNAME;
				filingField = "author_pers_filing";
				romanFilingField = "author_pers_roman_filing";
			} else if ( f.mainTag.endsWith("10")) {
				ht = HeadingType.CORPNAME;
				filingField = "author_corp_filing";
				romanFilingField = "author_corp_roman_filing";
			} else if ( f.mainTag.endsWith("11")) {
				ht = HeadingType.EVENT;
				filingField = "author_event_filing";
				romanFilingField = "author_event_roman_filing";
			} else {
				ht = null; filingField = null; romanFilingField = null;
			}

			// romanFilingField is a staff-facing search endpoint for authority control maintenance.
			if ( romanFilingField != null && ! f.tag.equals("880") && ! isCJK(facet) )
				sfs.add(new SolrField( romanFilingField, filing ));

			boolean includeInAuthor = ! rs.isProvenance().equals(ProvStatus.PROV_ONLY);
			boolean includeInProvenance = ! rs.isProvenance().equals(ProvStatus.NONE);
			boolean displayAsFMO = rs.isProvenance().equals(ProvStatus.PROV_ONLY);


			if (includeInProvenance) {
				rs.remove("former owner");
				String d = NameUtils.displayValue( f, rs, true );
				if (displayAsFMO)
					sfs.add(new SolrField( "former_owner_display" , d));
				sfs.add(new SolrField( "former_owner_t", d));
			}

			if ( ! includeInAuthor) return sfs;

			sfs.add(new SolrField( (isMainAuthor)?"author_display":"author_addl_display", display ));
			sfs.add(new SolrField( (isCJK)?
					(isMainAuthor)?"author_t_cjk":"author_addl_t_cjk":
						(isMainAuthor)?"author_t":"author_addl_t", display ));
			sfs.add(new SolrField( "author_facet", NameUtils.getFacetForm( facet )));
			if (filingField != null)
				sfs.add(new SolrField( filingField, filing ));

			final Map<String,Object> json = new LinkedHashMap<>();
			json.put("name1", display);
			json.put("search1", ctsVals.author);
			json.put("relator", (new RelatorSet(f)).toString() );
			if (ht != null) json.put("type", ht.toString());
			final AuthorityData authData = (ht != null) 
					? new AuthorityData(config,ctsVals.author,ht) : new AuthorityData(false);
			json.put("authorizedForm", authData.authorized);
			final ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream, json);
			sfs.add(new SolrField((isMainAuthor)
					?"author_json":"author_addl_json",jsonstream.toString("UTF-8")));

			if (authData.authorized && authData.alternateForms != null)
				for (final String altForm : authData.alternateForms) {
					sfs.add(new SolrField("authority_author_t",altForm));
					if (isCJK(altForm))
						sfs.add(new SolrField("authority_author_t_cjk",altForm));
				}
		}
		return sfs;
	}

}
