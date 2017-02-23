package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

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
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

/**
 * Generate instrumentation_display values from 382 fields, according to display recommendations at
 * http://musicoclcusers.org/wp-content/uploads/WCD_Medium_Report_201504291.pdf
 * Subfields $r and $t are recently added fields, and are not included in this display logic.
 * DISCOVERYACCESS-1608
 */
public class InstrumentationRSTF implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();
		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			rec.addDataFieldResultSet(rs,"382");
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();

		for( FieldSet fs: sortedFields.values() ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();

			for (DataField f: fs.fields) {
				String total_performers = null;
				StringBuilder sb = new StringBuilder();
				boolean forAppended = false;
				for ( Subfield sf : f.subfields.values() ) {
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
				if (f.tag.equals("880"))
					values880.add(sb.toString());
				else
					valuesMain.add(sb.toString());
			}
			for ( String s : values880 )
				addField(fields,"instrumentation_display",s);
			for ( String s : valuesMain )
				addField(fields,"instrumentation_display",s);
		}

		return fields;
	}

	private static final String FOR = "For ";
}
