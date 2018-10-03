package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * processing 510 notes into references_display, indexed_by_display, 
 * indexed_in_its_entirety_by_display, and indexed_selectively_by_display
 * 
 */
public class CitationReferenceNote implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("510"); }

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {
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
