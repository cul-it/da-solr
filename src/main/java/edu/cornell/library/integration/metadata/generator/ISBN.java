package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Generate isbn_display and isbn_t fields from 020 MARC field data.
 * DISCOVERYACCESS-2661
 * | q data handled according to 2016 updated PCC standard per DISCOVERYACCESS-3242
 */
public class ISBN implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("020"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		SolrFields vals = new SolrFields();
		for (DataField f: rec.matchSortAndFlattenDataFields()) {
			StringBuilder sbDisplay = new StringBuilder();
			boolean aFound = false;
			Character prevSubfield = null; 
			for ( Subfield sf : f.subfields ) {
				if ( sf.value.isEmpty() ) continue;
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
					vals.add(new SolrField( "isbn_t", searchISBN ));

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
					while (":; ".contains(String.valueOf(sbDisplay.charAt(sbDisplay.length()-1))))
						sbDisplay.setLength(sbDisplay.length()-1);
					if (sbDisplay.charAt(sbDisplay.length()-1) != ')')
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
				vals.add(new SolrField("isbn_display",s));
			}
		}
		return vals;
	}
}
