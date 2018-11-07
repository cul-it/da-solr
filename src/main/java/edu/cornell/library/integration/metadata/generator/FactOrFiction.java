package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Using control fixed fields present in Book type MARC bib records to identify
 * Fact and Fiction records. Some control field values, such as 'poetry' are
 * ambiguous.
 */
public class FactOrFiction implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("leader","008"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		String chars6and7 = rec.leader.substring(6,8);
		String char33 = "";
		for (ControlField f : rec.controlFields)
			if (f.tag.equals("008") && f.value.length() >= 34)
				char33 = f.value.substring(33,34);

		SolrFields vals = new SolrFields();
		if (chars6and7.equalsIgnoreCase("aa") 
			|| chars6and7.equalsIgnoreCase("ac")
			|| chars6and7.equalsIgnoreCase("ad")
			|| chars6and7.equalsIgnoreCase("am")
			|| chars6and7.startsWith("t")
			) {
			if (char33.equals("0") ||
					char33.equalsIgnoreCase("i")) {
				vals.add(new SolrField("subject_content_facet","Non-Fiction (books)"));
			} else if (char33.equals("1") ||
					char33.equalsIgnoreCase("d") ||
					char33.equalsIgnoreCase("f") ||
					char33.equalsIgnoreCase("j")) {
				vals.add(new SolrField("subject_content_facet","Fiction (books)"));
			}
		}
		
		return vals;

	}
}
