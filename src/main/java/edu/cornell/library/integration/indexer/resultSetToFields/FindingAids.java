package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.utilities.CharacterSetUtils;

/**
 * Divvy contents of 555 notes into finding_aids_display, indexes_display, or general notes,
 * according to the value of the first indicator. All permutations go to notes_t and/or notes_t_cjk
 * for searching, because we don't currently need to distinguish.
 */
public class FindingAids implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Map<String,String> q2f = new HashMap<>();
		q2f.put("finding_aids_index_notes","555");
		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results,q2f);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( FieldSet fs: sets ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			String relation = null;
			Boolean cjk880Found = false;

			for (DataField f: fs.fields) {

				switch (f.ind1) {
				case '0': relation = "finding_aids_display"; break;
				case '8': relation = "notes"; break;
				default: relation = "indexes_display";
				}
				String value = f.concatenateSpecificSubfields("3abcdu");
				if (f.tag.equals("880")) {
					values880.add(value);
					if (f.getScript().equals(MarcRecord.Script.CJK))
						cjk880Found = true;
				} else
					valuesMain.add(value);
			}
			if (relation == null)
				continue;
			for ( String s : values880 ) {
				addField(fields,relation,s);
				if (cjk880Found) {
					addField(fields,"notes_t_cjk",s);
				} else {
					if (CharacterSetUtils.isCJK(s))
						addField(fields,"notes_t_cjk",s);
					addField(fields,"notes_t",s);
				}
			}
			for ( String s : valuesMain ) {
				addField(fields,relation,s);
				if (CharacterSetUtils.isCJK(s))
					addField(fields,"notes_t_cjk",s);
				addField(fields,"notes_t",s);
			}
		}

		return fields;
	}

}
