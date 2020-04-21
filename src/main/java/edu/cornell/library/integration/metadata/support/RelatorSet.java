package edu.cornell.library.integration.metadata.support;

import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.Subfield;

public class RelatorSet {

	Set<String> relators = new LinkedHashSet<>();

	private final String RelatorCodeURIPrefix = "http://id.loc.gov/vocabulary/relators/";
	public RelatorSet(DataField f) {
		boolean isEventField = f.mainTag.endsWith("11");
		for (Subfield sf : f.subfields) {
			if (sf.code.equals('4')) {
				String code;
				if ( sf.value.startsWith(this.RelatorCodeURIPrefix) )
					code = sf.value.substring(this.RelatorCodeURIPrefix.length()).toLowerCase().replaceAll("[^a-z]", "");
				else
					code = sf.value.toLowerCase().replaceAll("[^a-z]", "");
				try {
					this.relators.add(Relator.valueOf(code).toString());
				} catch (@SuppressWarnings("unused") IllegalArgumentException e) {
					Relator r = Relator.valueOfString(sf.value.toLowerCase().replaceAll("\\.", ""));
					if ( r != null ) {
						System.out.println("Relator value \""+sf.value+"\" provided in $4.");
						this.relators.add( r.toString() );
					} else
						System.out.println("Unexpected relator code: \""+sf.value+"\".");
				}
			}
			else if ((isEventField && sf.code.equals('j'))
					|| ( ! isEventField && sf.code.equals('e'))) {
				String relator = sf.value.toLowerCase();
				this.relators.add(removeTrailingPunctuation(relator,".,"));
			}
		}
	}
	public boolean isEmpty() {
		return this.relators.isEmpty();
	}
	@Override
	public String toString() {
		return String.join(", ",this.relators);
	}
	public static String validateForConcatWRelators( String s ) {
		if (s.endsWith("-,"))
			return s.substring(0,s.length()-1);
		Matcher m = neededTerminalPeriod.matcher(s);
		if ( m.matches() ) {
			if ( s.endsWith(",") || s.endsWith("-"))
				return s;
			return s+",";
		}
		if (s.endsWith(".,"))
			return s.substring(0,s.length()-2)+",";
		if (s.endsWith(".-"))
			return s.substring(0,s.length()-2)+"-";
		if (s.endsWith("."))
			return s.substring(0,s.length()-1)+",";
		if (s.endsWith(",") || s.endsWith("-"))
			return s;
		return s+",";
	}
	private static Pattern neededTerminalPeriod
		= Pattern.compile("(.*)(etc|Mrs|Inc|Sr|Jr|Spon|Co|Inc|Ltd|[A-Z])\\.([\\-,]?)$");

}
