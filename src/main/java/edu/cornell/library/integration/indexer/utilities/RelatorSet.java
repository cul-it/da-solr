package edu.cornell.library.integration.indexer.utilities;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

public class RelatorSet {

	Set<String> relators = new HashSet<String>();

	public RelatorSet(DataField f) {
		boolean isEventField = f.mainTag.endsWith("11");
		for (Subfield sf : f.subfields.values()) {
			if (sf.code.equals('4')) {
				String code = sf.value.toLowerCase().replaceAll("[^a-z]", "");
				try {
					relators.add(Relator.valueOf(code).toString());
				} catch (IllegalArgumentException e) {
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
		return StringUtils.join(this.relators,", ");
	}
	public static String validateForConcatWRelators( String orig ) {
		if (orig.endsWith("-,")) {
			orig = orig.substring(0, orig.length() - 1);
		} else if (orig.endsWith(",")
				|| orig.endsWith("-")) {
			// this is correct - do nothing
		} else {
			orig += ',';
		}
		return orig;
	}
}