package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Using control fixed fields present in Book type MARC bib records to identify
 * Fact and Fiction records. Some control field values, such as 'poetry' are
 * ambiguous.
 */
public class FactOrFiction implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {


		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = nodeToString(results.get("leader").nextSolution().get("leader"));
		JenaResultsToMarcRecord.addControlFieldResultSet( rec, results.get("eight") );

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("leader","008"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig unused ) {

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
