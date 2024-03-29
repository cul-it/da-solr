package edu.cornell.library.integration.metadata.support;

import java.io.IOException;
import java.sql.SQLException;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.generator.FactOrFiction;
import edu.cornell.library.integration.metadata.generator.SolrFieldGenerator;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * If a work is on the New & Noteworthy Books shelf at Olin, the holdings call number
 * represents its eventual library of congress filing in the stacks, but not its current
 * filing on the shelf. We can display the actual shelf location to patrons, however.
 */
public class ModifyCallNumbers {

	public static String modify(MarcRecord bib , String orig)
			throws SQLException, IOException {

		if (orig.trim().startsWith("New & Noteworthy Books")) {
			boolean fiction = isFiction(bib);
			String author = getAuthorPrefix(bib);
			if (orig.endsWith("+"))
				return "New & Noteworthy Books Oversize "+author+" ++";
			return "New & Noteworthy Books "+((fiction) ? "Fiction " : "Non-Fiction ")+author;
		}
		return orig;
	}

	private static boolean isFiction (MarcRecord bib) throws SQLException, IOException {
		SolrFieldGenerator factOrFictionGenerator = new FactOrFiction();
		SolrFields factOrFictionFields = factOrFictionGenerator.generateSolrFields(bib, null);
		for (SolrField f : factOrFictionFields.fields)
			if (f.fieldName.equals("subject_content_facet")
					&& f.fieldValue.equals("Fiction (books)"))
				return true;
		return false;
	}

	private static String getAuthorPrefix (MarcRecord bib) {
		String author = null;
		for (DataField f : bib.dataFields)
			if (f.tag.equals("100") || f.tag.equals("110"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a'))
						author = sf.value;

		if (author == null) return null;
		// Shorten to 4 characters
		if (author.length() > 4)
			author = author.substring(0, 4);
		return author.toUpperCase();
	}
}
