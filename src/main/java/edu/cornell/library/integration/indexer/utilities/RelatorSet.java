package edu.cornell.library.integration.indexer.utilities;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.LinkedHashSet;
import java.util.Set;

import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

public class RelatorSet {

	Set<String> relators = new LinkedHashSet<>();

	public RelatorSet(DataField f) {
		boolean isEventField = f.mainTag.endsWith("11");
		for (Subfield sf : f.subfields.values()) {
			if (sf.code.equals('4')) {
				String code = sf.value.toLowerCase().replaceAll("[^a-z]", "");
				try {
					relators.add(Relator.valueOf(code).toString());
				} catch (@SuppressWarnings("unused") IllegalArgumentException e) {
					Relator r = Relator.valueOfString(sf.value.toLowerCase().replaceAll("\\.", ""));
					if ( r != null ) {
						System.out.println("Relator value \""+sf.value+"\" provided in $4.");
						relators.add( r.toString() );
					} else
						System.out.println("Unexpected relator code: \""+sf.value+"\".");
				}
			}
			else if ((isEventField && sf.code.equals('j'))
					|| ( ! isEventField && sf.code.equals('e'))) {
				String relator = sf.value.toLowerCase();
				relators.add(removeTrailingPunctuation(relator,".,"));
			}
		}
	}
	public boolean isEmpty() {
		return this.relators.isEmpty();
	}
	public String toString() {
		return String.join(", ",this.relators);
	}
	public static String validateForConcatWRelators( String s ) {
		if (s.endsWith("-,")) {
			s = s.substring(0,s.length()-1);
		} else if (s.endsWith(",")
				|| s.endsWith("-")) {
			// this is correct - do nothing
		} else {
			s += ',';
		}
		return s;
	}
}
