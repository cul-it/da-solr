package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.utilities.CharacterSetUtils;

public class Notes implements ResultSetToFields {
	@Override
	public Map<String, SolrInputField> toFields(Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( FieldSet fs: sets ) {
			SolrFieldValueSet vals = generateSolrFields( fs );
			if (vals.displayField != null)
				for (String dv : vals.displayValues)
					ResultSetUtilities.addField(fields,vals.displayField,dv);
			for (String sv : vals.searchValues) {
				ResultSetUtilities.addField(fields,"notes_t",sv);
				if (CharacterSetUtils.hasCJK(sv))
					ResultSetUtilities.addField(fields,"notes_t_cjk",sv);
			}
		}
		return fields;
	}

	public static SolrFieldValueSet generateSolrFields( FieldSet fs ) {
		SolrFieldValueSet vals = new SolrFieldValueSet();
		vals.displayField = "notes";
		for (DataField f : fs.fields) {
			String displaySubfields = null, searchSubfields = null;
			switch (Integer.valueOf(f.mainTag)) {
			case 300:
				displaySubfields = "3abcefg";
				vals.displayField = "description_display";
				break;
			case 362:    displaySubfields = "a";         searchSubfields = "a";        break;
			case 500:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 501:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 502:
				displaySubfields = "3abcdgo";
				searchSubfields = "abcdgo";
				vals.displayField = "thesis_display";
				break;
			case 503:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 504:    displaySubfields = "3ab";       searchSubfields = "ab";       break;
			case 506:
				displaySubfields = "3abce";
				searchSubfields = "3abce";
				vals.displayField = "restrictions_display";
				break;
			case 508:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 510:    displaySubfields = "abcux3";    searchSubfields = "abcux3";   break;
			case 511:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 513:    displaySubfields = "3ab";       searchSubfields = "ab";       break;
			case 515:    displaySubfields = "a";         searchSubfields = "a";        break;
			case 518:    displaySubfields = "3adop";     searchSubfields = "adop";     break;
			case 520:
				displaySubfields = "3abc";
				searchSubfields = "abc";
				vals.displayField = "summary_display";
				break;
			case 521:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 522:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 523:    displaySubfields = "3a";        searchSubfields = "a";        break;
			case 524:
				displaySubfields = "a3";
				searchSubfields = "a3";
				vals.displayField = "cite_as_display";
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
				vals.displayField = "description_display";
				break;
			case 540:
				displaySubfields = "3abcu";
				searchSubfields = "3abcu";
				vals.displayField = "restrictions_display";
				break;
			case 541:
				if (" 1".contains(f.ind1.toString())) {
					displaySubfields = "3ac";
					vals.displayField = "donor_display";
				}
				break;
			case 544:    displaySubfields = "3ad";       searchSubfields = "ad";       break;
			case 545:
				displaySubfields = "3abcu";
				searchSubfields = "3abcu";
				vals.displayField = "historical_note_display";
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
			case 856:    displaySubfields = "m";         searchSubfields = "m";        break;
			case 940:    displaySubfields = "a";         searchSubfields = "a";        break;

			}
			if (displaySubfields != null) {
				String displayValue = f.concatenateSpecificSubfields(displaySubfields);
				if (! displayValue.isEmpty()) vals.displayValues.add(displayValue);
			}
			if (searchSubfields != null) {
				String searchValue = f.concatenateSpecificSubfields(searchSubfields);
				if (! searchValue.isEmpty()) vals.searchValues.add(searchValue);
			}
		}

		return vals;
	}

	public static class SolrFieldValueSet {
		String displayField = null;
		List<String> displayValues = new ArrayList<>();
		List<String> searchValues = new ArrayList<>();
	}

}
