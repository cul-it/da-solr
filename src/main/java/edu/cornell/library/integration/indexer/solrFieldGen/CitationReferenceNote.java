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

/**
 * processing 510 notes into references_display, indexed_by_display, 
 * indexed_in_its_entirety_by_display, and indexed_selectively_by_display
 * 
 */
public class CitationReferenceNote implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("field510"));

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
		String relation = null;
		SolrFields v = new SolrFields();
		for (DataField f: rec.matchSortAndFlattenDataFields()) {
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

			String value = f.concatenateSpecificSubfields("abcux3");
			if (relation != null)
				v.add( new SolrField ( relation, value ));
			v.add( new SolrField ( "notes_t", value ));
		}
		return v;
	}

}
