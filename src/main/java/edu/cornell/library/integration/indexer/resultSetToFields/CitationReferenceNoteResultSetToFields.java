package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing 510 notes into references_display, indexed_by_display, 
 * indexed_in_its_entirety_by_display, and indexed_selectively_by_display
 * 
 */
public class CitationReferenceNoteResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( FieldSet fs: sets ) {
			SolrFieldValueSet solrFields = generateSolrFields( fs );
			for (Entry<String,List<String>> e : solrFields.values.entrySet())
				for (String value : e.getValue())
					addField(fields,e.getKey(),value);

		}
		return fields;
	}

	public static SolrFieldValueSet generateSolrFields( FieldSet fs ) {
		Set<String> values880 = new HashSet<>();
		Set<String> valuesMain = new HashSet<>();
		String relation = null;
		for (DataField f: fs.fields) {
			if (relation == null) {
				if (f.ind1.equals('4')) relation = "references";
				else if (f.ind1.equals('3')) relation = "references";
				else if (f.ind1.equals(' ')) relation = "references";
				else if (f.ind1.equals('2')) relation = "indexed_selectively_by";
				else if (f.ind1.equals('1')) relation = "indexed_in_its_entirety_by";
				else if (f.ind1.equals('0')) relation = "indexed_by";
			}
			if (f.tag.equals("880"))
				values880.add(f.concatenateSpecificSubfields("abcux3"));
			else
				valuesMain.add(f.concatenateSpecificSubfields("abcux3"));
		}
		SolrFieldValueSet v = new SolrFieldValueSet();
		if (relation != null) {
			for (String s: values880)
				v.put(relation+"_display",s);
			for (String s: valuesMain)
				v.put(relation+"_display",s);
		}
		return v;
	}

	public static class SolrFieldValueSet {
		Map<String,List<String>> values = new HashMap<>();
		public void put( String key, String value ) {
			if (values.containsKey(key)) {
				values.get(key).add(value);
				return;
			}
			List<String> keyvals = new ArrayList<>();
			keyvals.add(value);
			values.put(key, keyvals);
		}
	}
}
