package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.CharacterSetUtils;
import edu.cornell.library.integration.utilities.IndexingUtilities;

public class SimpleProc implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("notes"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() {
		return Arrays.asList(
				"010","022","024","028","035","210","222","242","243","246","247","250","255","300","310",
				"362","500","501","502","503","504","506","508","511","513","515","518","520","521","522",
				"523","524","525","527","530","533","534","535","537","538","540","541","544","545","547",
				"550","556","561","565","567","570","580","582","773","856","899","902","903","940");
	}

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {

		SolrFields sfs = new SolrFields();

		for (DataField f : rec.matchSortAndFlattenDataFields()) {
			String displayField = "notes";
			String searchField = "notes_t";
			String cjkSearchField = "notes_t_cjk";
			String displaySubfields = null;
			String searchSubfields = null;
			String displayCleanupChars = null;
			Boolean titleMode = false;
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
				searchSubfields = "al";
				break;
			case 24:
				searchField = "id_t";
				cjkSearchField = "id_t_cjk";
				displayField = "other_identifier_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 28:
				searchField = "id_t";
				cjkSearchField = "id_t_cjk";
				displayField = "publisher_number_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 35:
				searchField = "id_t";
				cjkSearchField = "id_t_cjk";
				displayField = "other_id_display";
				displaySubfields = "a";
				searchSubfields = "a";
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
			case 511:    displaySubfields = "3a";        searchSubfields = "a";        break;
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
			case 582:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 773:
				displayField = "in_display";
				displaySubfields = "abdghikmnopqrstuw";
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
				displayField = "donor_display";
				displaySubfields = "b";
				break;
			case 903:
				searchField = "barcode_addl_t";
				cjkSearchField = "barcode_addl_t";
				searchSubfields = "p";
				break;
			case 940:    displaySubfields = "a";         searchSubfields = "a";        break;

			}
			if (displaySubfields != null) {
				String displayValue = f.concatenateSpecificSubfields(displaySubfields);
				if (displayCleanupChars != null)
					displayValue = IndexingUtilities.removeTrailingPunctuation( displayValue, displayCleanupChars );
				if (! displayValue.isEmpty())
					sfs.add(new SolrField(displayField,displayValue));
			}
			if (searchSubfields != null) {
				String searchValue = f.concatenateSpecificSubfields(searchSubfields);
				if (! searchValue.isEmpty()) {
					if (f.getScript().equals(DataField.Script.CJK))
						sfs.add(new SolrField(cjkSearchField,searchValue));
					else {
						if (CharacterSetUtils.hasCJK(searchValue))
							sfs.add(new SolrField(cjkSearchField,searchValue));
						sfs.add(new SolrField(searchField,CharacterSetUtils.standardizeApostrophes(searchValue)));
						if (titleMode)
							sfs.add(new SolrField(searchField,CharacterSetUtils.standardizeApostrophes(
									f.getStringWithoutInitialArticle(searchValue))));
					}
				}
			}
		}

		return sfs;
	}
}
