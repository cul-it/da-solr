package edu.cornell.library.integration.indexer.utilities;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeAllPunctuation;

import java.text.Normalizer;

public class BrowseUtils {
	
	public static enum HeadTypeDesc {
		PERSNAME("Personal Name"),
		CORPNAME("Corporate Name"),
		EVENT("Event"),
		GENHEAD("General Heading"),
		TOPIC("Topical Term"),
		GEONAME("Geographic Name"),
		CHRONTERM("Chronological Term"),
		GENRE("Genre/Form Term"),
		MEDIUM("Medium of Performance");
		
		private String string;
		
		private HeadTypeDesc(String name) {
			string = name;
		}

		public String toString() { return string; }
	}
	
	public static enum HeadType {
		AUTHOR("author"),
		SUBJECT("subject"),
		AUTHORTITLE("authortitle");
		
		private String string;
		
		private HeadType(String name) {
			string = name;
		}

		public String toString() { return string; }
	}	

	public static enum RecordSet {
		NAME("name"),
		SUBJECT("subject"),
		SERIES("series"), /*Not currently implementing series header browse*/
		NAMETITLE("nametitle");
		private String string;
		
		private RecordSet(String name) {
			string = name;
		}

		public String toString() { return string; }
	}


	
	public static String getSortHeading(String heading) {
		
		
		
		/* We will first normalize the unicode. For sorting, we will use 
		 * "compatibility decomposed" form (NFKD). Decomposed form will make it easier
		 * to match and remove diacritics, while compatibility form will further
		 * drop encoding that are for use in formatting only and should not affect
		 * sorting. For example, æ => ae
		 * See http://unicode.org/reports/tr15/ Figure 6
		 */
		String step1 = Normalizer.normalize(heading, Normalizer.Form.NFKD).
				replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		
		/* removeAllPunctuation() will strip punctuation. We replace hyphens with spaces
		 * first so hyphenated words will sort as though the space were present.
		 */
		String step2 = step1.toLowerCase().replaceAll("-", " ");
		String sortHeading = removeAllPunctuation(step2);
		
		// Finally, collapse sequences of spaces into single spaces:
		return sortHeading.trim().replaceAll("\\s+", " ");
	}

}
