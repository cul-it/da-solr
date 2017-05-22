package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;

/**
 * processing 510 notes into references_display, indexed_by_display, 
 * indexed_in_its_entirety_by_display, and indexed_selectively_by_display
 * 
 */
public class CitationReferenceNote implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( FieldSet fs: sets ) {

			SolrFields vals = generateSolrFields( fs );
			for ( SolrField f : vals.fields )
				ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		}
		return fields;
	}

	public static SolrFields generateSolrFields( FieldSet fs ) {
		String relation = null;
		SolrFields v = new SolrFields();
		for (DataField f: fs.getFields()) {
			if (relation == null)
				switch (f.ind1) {
				case '4':
				case '3':
				case ' ':
					relation = "references_display";  break;
				case '2':
					relation = "indexed_selectively_by_display"; break;
				case '1':
					relation = "indexed_in_its_entirety_by_display"; break;
				case '0':
					relation = "indexed_by_display"; break;
				}

			if (relation != null)
				v.fields.add( new SolrField ( relation, f.concatenateSpecificSubfields("abcux3") ));
		}
		return v;
	}

}
