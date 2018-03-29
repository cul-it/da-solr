package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Divvy contents of 555 notes into finding_aids_display, indexes_display, or general notes,
 * according to the value of the first indicator. All permutations go to notes_t and/or notes_t_cjk
 * for searching, because we don't currently need to distinguish.
 */
public class FindingAids implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, Config config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("finding_index_notes"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("555"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		SolrFields sfs = new SolrFields();
		for (DataField f: rec.matchSortAndFlattenDataFields()) {

			String relation = null;

			switch (f.ind1) {
			case '0': relation = "finding_aids_display"; break;
			case '8': relation = "notes"; break;
			default: relation = "indexes_display";
			}

			String value = f.concatenateSpecificSubfields("3abcdu");
			sfs.add(new SolrField(relation,value));
			if (f.tag.equals("880") && f.getScript().equals(DataField.Script.CJK))
				sfs.add(new SolrField("notes_t_cjk",value));
			else
				sfs.add(new SolrField("notes_t",value));
		}

		return sfs;
	}

}
