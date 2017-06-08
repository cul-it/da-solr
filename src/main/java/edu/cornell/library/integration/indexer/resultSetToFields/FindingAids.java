package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Divvy contents of 555 notes into finding_aids_display, indexes_display, or general notes,
 * according to the value of the first indicator. All permutations go to notes_t and/or notes_t_cjk
 * for searching, because we don't currently need to distinguish.
 */
public class FindingAids implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.addDataFieldResultSet(results.get("finding_index_notes"));

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

		DataFieldSet fs = rec.matchSortAndFlattenDataFields("555");
		SolrFields sfs = new SolrFields();

		for (DataField f: fs.getFields()) {

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
