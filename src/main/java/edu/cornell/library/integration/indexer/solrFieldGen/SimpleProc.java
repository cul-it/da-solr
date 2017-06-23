package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.CharacterSetUtils;

public class SimpleProc implements ResultSetToFields {

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

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {

		SolrFields sfs = new SolrFields();

		for (DataField f : rec.matchSortAndFlattenDataFields()) {
			String displayField = "notes";
			String searchField = "notes_t";
			String displaySubfields = null;
			String searchSubfields = null;
			switch (Integer.valueOf(f.mainTag)) {
			case 10:
				searchField = "lc_controlnum_s";
				displayField = "lc_controlnum_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 22:
				searchField = "issn_t";
				displayField = "issn_display";
				displaySubfields = "a";
				searchSubfields = "al";
				break;
			case 24:
				searchField = "id_t";
				displayField = "other_identifier_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 28:
				searchField = "id_t";
				displayField = "publisher_number_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 35:
				searchField = "id_t";
				displayField = "other_id_display";
				displaySubfields = "a";
				searchSubfields = "a";
				break;
			case 250:
				displayField = "edition_display";
				displaySubfields = "3ab";
				break;
			case 255:
				displayField = "map_format_display";
				displaySubfields = "abcdefg";
				break;
			case 300:
				displaySubfields = "3abcefg";
				displayField = "description_display";
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
				displayField = "eightninenine_display";
				displaySubfields = "ab";
				searchSubfields = "ab";
				break;
			case 902:
				displayField = "donor_display";
				displaySubfields = "b";
				break;
			case 903:
				searchField = "barcode_t";
				searchSubfields = "p";
				break;
			case 940:    displaySubfields = "a";         searchSubfields = "a";        break;

			}
			if (displaySubfields != null) {
				String displayValue = f.concatenateSpecificSubfields(displaySubfields);
				if (! displayValue.isEmpty())
					sfs.add(new SolrField(displayField,displayValue));
			}
			if (searchSubfields != null) {
				String searchValue = f.concatenateSpecificSubfields(searchSubfields);
				if (! searchValue.isEmpty()) {
					sfs.add(new SolrField(searchField,searchValue));
					if (!searchField.endsWith("_s") && CharacterSetUtils.isCJK(searchValue))
						sfs.add(new SolrField(searchField+"_cjk",searchValue));
				}
			}
		}

		return sfs;
	}
}
