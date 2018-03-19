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
import edu.cornell.library.integration.marc.Subfield;

/**
 * Generate instrumentation_display values from 382 fields, according to display recommendations at
 * http://musicoclcusers.org/wp-content/uploads/WCD_Medium_Report_201504291.pdf
 * Subfields $r and $t are recently added fields, and are not included in this display logic.
 * DISCOVERYACCESS-1608
 */
public class Instrumentation implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, Config config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("instrumentation"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("382"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		SolrFields sfs = new SolrFields();
		for( DataField f: rec.matchSortAndFlattenDataFields() ) {

			String total_performers = null;
			StringBuilder sb = new StringBuilder();
			boolean forAppended = false;
			for ( Subfield sf : f.subfields ) {
				switch (sf.code) {
				case 'a':
					if (forAppended) sb.append("; ");
					else{ sb.append(FOR); forAppended = true; }
					sb.append(sf.value);
					break;
				case 'b':
					if (forAppended) sb.append("; ");
					else{ sb.append(FOR); forAppended = true; }
					sb.append("solo ").append(sf.value);
					break;
				case 'd':
					if (forAppended) sb.append('/');
					else{ sb.append(FOR); forAppended = true; }
					sb.append(sf.value);
					break;
				case 'n':
				case 'e':
					try {
						int count = Integer.valueOf(sf.value);
						if (count > 1) {
							if (forAppended) sb.append(' ');
							else{ sb.append(FOR); forAppended = true; }
							sb.append('(').append(count).append(')');
						}
					} catch (@SuppressWarnings("unused") NumberFormatException e) {
						System.out.println("382$n/e is not an integer ("+sf.value+").");
					}
					break;
				case 'p':
					if (forAppended) sb.append(" or ");
					else{ sb.append(FOR); forAppended = true; }
					sb.append(sf.value);
					break;
				case 's':
					total_performers = sf.value;
					break;
				case '3':
				case 'v':
					if (forAppended)
						sb.append(" [").append(sf.value).append(']');
					else
						sb.append('[').append(sf.value).append("]: ");
					break;
				}
			}
			if (total_performers != null) {
				if (sb.length() > 0) sb.append(". ");
				sb.append("Total performers: ").append(total_performers);
			}

			String instrumentation = sb.toString();
			sfs.add(new SolrField("instrumentation_display",instrumentation));
			sfs.add(new SolrField("notes_t",instrumentation));
		}
		return sfs;
	}

	private static final String FOR = "For ";
}
