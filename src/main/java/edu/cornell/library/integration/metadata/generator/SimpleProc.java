package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.CharacterSetUtils;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.library.integration.utilities.SolrFields;

public class SimpleProc implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.6.2"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList(
				"010","022","074","086","210","222","242","243","246","247","250","255","300","310",
				"362","500","501","502","503","504","506","508","511","513","515","518","520","521","522",
				"523","524","525","527","530","533","534","535","537","538","540","541","544","545","547",
				"550","556","561","565","567","570","580","581","582","586","773","856","899","902","903","940");
	}

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {

		SolrFields sfs = new SolrFields();
		boolean f300e = false;

		for (DataField f : rec.matchSortAndFlattenDataFields()) {
			String displayField = "notes_display";
			String searchField = "notes_t";
			String cjkSearchField = "notes_t_cjk";
			String displaySubfields = null;
			String searchSubfields = null;
			String displayCleanupChars = null;
			Boolean titleMode = false;
			Boolean separateSubfields = false;
			switch (Integer.valueOf(f.mainTag)) {
			case 10:
				searchField = "lc_controlnum_s";
				cjkSearchField = "lc_controlnum_s";
				displayField = "lc_controlnum_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 22:
				searchField = "issn_t";
				cjkSearchField = "issn_t_cjk";
				displayField = "issn_display";
				displaySubfields = "a";
				searchSubfields = "almyz";
				break;
			case 74:
			case 86:
				searchField = "id_left_chunked";
				cjkSearchField = "id_left_chunked";
				searchSubfields = "az";
				break;
			case 210:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				searchSubfields = "ab";
				break;
			case 222:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				searchSubfields = "ab";
				titleMode = true;
				break;
			case 242:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				searchSubfields = "abnp";
				titleMode = true;
				break;
			case 243:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				displayField = "title_other_display";
				displaySubfields = "adfgklmnoprs";
				searchSubfields = "abcdefgklmnopqrs";
				displayCleanupChars = ":/ ";
				titleMode = true;
				break;
			case 246:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				displayField = "title_other_display";
				displaySubfields = "iabfnpg";
				searchSubfields = "abcdefgklmnopqrs";
				displayCleanupChars = ":/ ";
				break;
			case 247:
				searchField = "title_addl_t";
				cjkSearchField = "title_addl_t_cjk";
				displayField = "continues_display";
				displaySubfields = "abfgnpx";
				searchSubfields = "abcdefgnp";
				displayCleanupChars = ":/ ";
				break;
			case 250:
				displayField = "edition_display";
				displaySubfields = "3ab";
				searchSubfields = "ab";
				break;
			case 255:
				displayField = "map_format_display";
				displaySubfields = "abcdefg";
				break;
			case 300:
				displaySubfields = "3abcefg";
				displayField = "description_display";
				searchSubfields = "abcefg";
				for (Subfield sf: f.subfields) if (sf.code.equals('e')) f300e = true;
				break;
			case 310:
				displaySubfields = "ab";
				displayField = "frequency_display";
				break;
			case 362:    displaySubfields = "a";         searchSubfields = "a";        break;
			case 500:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 501:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 502:
				displaySubfields = "3abcdgo";
				searchSubfields = "abcdgo";
				displayField = "thesis_display";
				break;
			case 503:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 504:    displaySubfields = "3ab";       searchSubfields = "ab";       break;
			case 506:
				displaySubfields = "3abce";
				searchSubfields = "3abce";
				displayField = "restrictions_display";
				break;
			case 508:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 511:
				displaySubfields = "3a";
				searchSubfields = "a";
				if (f.ind1.equals('1')) displayField = "cast_display";
				break;
			case 513:    displaySubfields = "3ab";       searchSubfields = "ab";       break;
			case 515:    displaySubfields = "a";         searchSubfields = "a";        break;
			case 518:    displaySubfields = "3adop";     searchSubfields = "adop";     break;
			case 520:
				displaySubfields = "3abc";
				searchSubfields = "abc";
				displayField = "summary_display";
				break;
			case 521:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 522:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 523:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 524:
				displaySubfields = "a3";
				searchSubfields = "a3";
				displayField = "cite_as_display";
				break;
			case 525:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 527:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 530:    displaySubfields = "abc3";      searchSubfields = "abc3";     break;
			case 533:    displaySubfields = "aebcdfn3";  searchSubfields = "aebcdfn3"; break;
			case 534:    displaySubfields = "3abcefmpt"; searchSubfields = "abcefmpt"; break;
			case 535:    displaySubfields = "abcd3";     searchSubfields = "abcd3";    break;
			case 537:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 538:
				displaySubfields = "3a";
				searchSubfields = "a";
				displayField = "description_display";
				break;
			case 540:
				displaySubfields = "3abcu";
				searchSubfields = "3abcu";
				displayField = "restrictions_display";
				break;
			case 541:
				if (" 1".contains(f.ind1.toString())) {
					displaySubfields = "3ac";
					displayField = "donor_display";
					displayCleanupChars = "; ";
				}
				break;
			case 544:    displaySubfields = "3ad";       searchSubfields = "ad";       break;
			case 545:
				displaySubfields = "3abcu";
				searchSubfields = "3abcu";
				displayField = "historical_note_display";
				break;
			case 547:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 550:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 556:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 561:    displaySubfields = "ab3";       searchSubfields = "ab3";      break;
			case 565:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 567:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 570:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 580:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 581:
				displayField = "works_about_display";
				displaySubfields = "3az";
				searchSubfields = "a";
				break;
			case 582:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 586:
				displayField = "awards_display";
				displaySubfields = "3a";
				searchSubfields = "a";
				break;
			case 773:
				if ( f.subfields.size() > 0 && ! f.subfields.first().value.startsWith("EBSCO Publication Find")) {
					displayField = "in_display";
					displaySubfields = "abdghikmnopqrstuw";
				}
				searchSubfields = "abcdghiknopqstuwxyz";
				break;
			case 856:    displaySubfields = "m";         searchSubfields = "m";        break;
			case 899:
				searchField = "eightninenine_t";
				cjkSearchField = "eightninenine_t_cjk";
				displayField = "eightninenine_display";
				displaySubfields = "ab";
				searchSubfields = "ab";
				break;
			case 902:
				separateSubfields = true;
				displayField = "donor_display";
				searchField = "donor_t";
				displaySubfields = "b";
				searchSubfields = "ab";
				break;
			case 903:
				searchField = "barcode_addl_t";
				cjkSearchField = "barcode_addl_t";
				searchSubfields = "p";
				break;
			case 940:    displaySubfields = "a";         searchSubfields = "a";        break;

			}
			if (displaySubfields != null) {
				if ( separateSubfields ) {
					List<String> displayValues = f.valueListForSpecificSubfields(displaySubfields);
					for (String displayValue : displayValues) {
						if (displayCleanupChars != null)
							displayValue = IndexingUtilities.removeTrailingPunctuation( displayValue, displayCleanupChars );
						if (! displayValue.isEmpty())
							sfs.add(displayField,displayValue);
					}
				} else {
					String displayValue = f.concatenateSpecificSubfields(displaySubfields);
					if (displayCleanupChars != null)
						displayValue = IndexingUtilities.removeTrailingPunctuation( displayValue, displayCleanupChars );
					if (! displayValue.isEmpty())
						sfs.add(displayField,displayValue);
				}
			}
			if (searchSubfields != null) {
				String searchValue = f.concatenateSpecificSubfields(searchSubfields);
				if (! searchValue.isEmpty()) {
					if (f.getScript().equals(DataField.Script.CJK))
						sfs.add(cjkSearchField,searchValue);
					else {
						if (CharacterSetUtils.hasCJK(searchValue))
							sfs.add(cjkSearchField,searchValue);
						sfs.add(searchField,CharacterSetUtils.standardizeApostrophes(searchValue));
						if (titleMode)
							sfs.add(searchField,CharacterSetUtils.standardizeApostrophes(
									f.getStringWithoutInitialArticle(searchValue)));
					}
				}
			}
		}
		if (f300e) sfs.add("f300e_b",true);

		return sfs;
	}

	public SolrFields generateNonMarcSolrFields(Map<String,Object> instance, Config unused ) {
		Map<String,String> fieldsToMap = new HashMap<>();
		fieldsToMap.put("editions", "edition_display");
		fieldsToMap.put("physicalDescriptions", "description_display");
		SolrFields vals = new SolrFields();
		for (Entry<String,String> e : fieldsToMap.entrySet())
			if (instance.containsKey(e.getKey()))
				for (String edition : (List<String>)instance.get(e.getKey()))
					if ( ! edition.isBlank()) {
						vals.add(e.getValue(), edition);
						vals.add("notes_t", edition);
					}
		if (instance.containsKey("notes")) {
			for (Map<String,Object> note : (List<Map<String,Object>>)instance.get("notes")) {

				if ((boolean) note.getOrDefault("staffOnly", false)) continue;
				if ( ! note.containsKey("instanceNoteTypeId")) continue;
				String noteValue = (String)note.getOrDefault("note", null);
				if (noteValue == null || noteValue.isBlank()) continue;

				String displayField = null;
				switch( SupportReferenceData.instanceNoteTypes.getName( (String)note.get("instanceNoteTypeId"))) {
				case "General note":
				case "With note":
				case "Bibliography note":
					displayField = "notes_display";  break;
				case "Dissertation note":
					displayField = "thesis_display"; break;
				case "Summary":
					displayField = "summary_display";break;
				}
				if (displayField != null) {
					vals.add(displayField, noteValue);
					vals.add("notes_t", noteValue);
				}
			}
		}
		if (instance.containsKey("alternativeTitles")) {
			for (Map<String,String> altTitle : (List<Map<String,String>>)instance.get("alternativeTitles")) {

				if ( ! altTitle.containsKey("alternativeTitleTypeId")) continue;
				String titleValue = (String)altTitle.getOrDefault("alternativeTitle", null);
				if (titleValue == null || titleValue.isBlank()) continue;

				String displayField = null;
				switch( SupportReferenceData.alternativeTitleTypes
						.getName((String)altTitle.get("alternativeTitleTypeId"))) {
				case "Other title":
				case "Variant title":
				case "Cover title":
				case "Caption title":
				case "Added title page title":
				case "Parallel title":
				case "Portion of title":
				case "Running title":
				case "Distinctive title":
					displayField = "title_other_display";
					break;
				}
				if (displayField != null) {
					vals.add(displayField, titleValue);
					vals.add("title_addl_t", titleValue);
				}
			}
		}
		if ( ! instance.containsKey("identifiers")) return vals;
		for (Map<String,String> identifier : (List<Map<String, String>>) instance.get("identifiers")) {

			if (! identifier.containsKey("identifierTypeId")) continue;
			String idValue = (String)identifier.getOrDefault("value", null);
			if (idValue == null || idValue.isBlank()) continue;

			switch(SupportReferenceData.identifierTypes.getName((String)identifier.get("identifierTypeId"))) {
			case "LCCN":
				vals.add("lc_controlnum_display", idValue);
				vals.add("lc_controlnum_s", idValue);
				break;
			case "ISSN":
				vals.add("issn_display", idValue);
				vals.add("issn_t", idValue);
				break;
			case "GPO item number":
				vals.add("id_left_chunked",idValue);
				break;
			}
		}
		return vals;
	}
}
