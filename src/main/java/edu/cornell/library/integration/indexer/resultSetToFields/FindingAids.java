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
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
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

		Collection<DataFieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( DataFieldSet fs: sets ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			String relation = null;
			Boolean cjk880Found = false;

			for (DataField f: fs.getFields()) {

				switch (f.ind1) {
				case '0': relation = "finding_aids_display"; break;
				case '8': relation = "notes"; break;
				default: relation = "indexes_display";
				}
				String value = f.concatenateSpecificSubfields("3abcdu");
				if (f.tag.equals("880")) {
					values880.add(value);
					if (f.getScript().equals(DataField.Script.CJK))
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
