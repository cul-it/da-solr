package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
 * Generate isbn_display and isbn_t fields from 020 MARC field data.
 * DISCOVERYACCESS-2661
 * | q data handled according to 2016 updated PCC standard per DISCOVERYACCESS-3242
 */
public class ISBN implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Map<String,SolrInputField> fields = new HashMap<>();
		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			rec.addDataFieldResultSet(rs,"020");
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();

		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {

			FieldSet fs = sortedFields.get(id);
			SolrFieldValueSet vals = generateSolrFields( fs );

			for ( String s : vals.display880 )
				addField(fields,"isbn_display",s,true);
			for ( String s : vals.displayMain )
				addField(fields,"isbn_display",s,true);
			for ( String s : vals.search880 )
				addField(fields,"isbn_t",s,true);
			for ( String s : vals.searchMain )
				addField(fields,"isbn_t",s,true);
		}

		return fields;
	}

	public static SolrFieldValueSet generateSolrFields( FieldSet fs ) {
		SolrFieldValueSet vals = new SolrFieldValueSet();
		for (DataField f: fs.fields) {
			StringBuilder sbDisplay = new StringBuilder();
			boolean aFound = false;
			Character prevSubfield = null; 
			for ( Subfield sf : f.subfields.values() ) {
				switch (sf.code) {
				case 'a':
					// Display values
					aFound = true;
					if (sbDisplay.length() > 0)
						sbDisplay.append(' ');
					sbDisplay.append(sf.value);

					// Search values
					String searchISBN ;
					int posOfSpace = sf.value.indexOf(' ');
					if (posOfSpace == -1) searchISBN = sf.value;
					else searchISBN = sf.value.substring(0, sf.value.indexOf(' '));
					if (f.tag.equals("880")) vals.search880.add(searchISBN);
					else vals.searchMain.add(searchISBN);

					break;
				case 'c':
					// Not currently displayed
					break;
				case 'q':
					// Displayed, not searched
					if (prevSubfield != null && prevSubfield.equals('q')) {

						sbDisplay.setLength(sbDisplay.length() - 1);
						sbDisplay.append(" ; ");
						if (sf.value.charAt(0) == '(') sbDisplay.append(sf.value.substring(1));
						else sbDisplay.append(sf.value);
					} else {

						if (sf.value.charAt(0) == '(') sbDisplay.append(' ');
						else sbDisplay.append(" (");
						sbDisplay.append(sf.value);
					}
					if (sf.value.charAt(sf.value.length()-1) == ';')
						sbDisplay.setLength(sbDisplay.length()-1);
					if (sf.value.charAt(sf.value.length()-1) != ')')
						sbDisplay.append(')');
					break;
				case 'z':
					// Not currently displayed
					break;
				}
				prevSubfield = sf.code;
			}
			if (aFound) {
				String s = removeTrailingPunctuation(sbDisplay.toString()," :;");
				if (f.tag.equals("880"))
					vals.display880.add(s);
				else
					vals.displayMain.add(s);
			}
		}
		return vals;
	}
	public static class SolrFieldValueSet {
		public Set<String> display880 = new LinkedHashSet<>();
		public Set<String> displayMain = new LinkedHashSet<>();
		public Set<String> search880 = new LinkedHashSet<>();
		public Set<String> searchMain = new LinkedHashSet<>();
	}
}
