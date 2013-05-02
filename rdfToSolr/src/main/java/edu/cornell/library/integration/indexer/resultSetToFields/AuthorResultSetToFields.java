package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

/**
 * processing main entry result set into fields author_display
 * This could theoretically result in multiple author_display values, which would
 * cause an error submitting to Solr. This can only happen in the case of catalog
 * error, but a post-processor will remove extra values before submission leading
 * to a successful submission.
 */
public class AuthorResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		
		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Set<String> values880 = new HashSet<String>();
			Set<String> valuesMain = new HashSet<String>();
			String dates = "";
		
			for (DataField f: dataFields) {
				String subfields;
				if (f.mainTag.equals("100")) {
					subfields = "abcq";
				} else {
					subfields = "abcefghijklmnopqrstuvwxyz";
				}
				if (! subfields.equals("")) {
					String value = f.concatenateSpecificSubfields(subfields);
					if (value.isEmpty()) continue;
					if (f.tag.equals("880")) {
						values880.add(value);
					} else {
						dates = removeTrailingPunctuation(f.concatenateSpecificSubfields("d"),".,");
						valuesMain.add(value);
					}
				}
			}
			if ((values880.size() == 1) && (valuesMain.size() == 1 )) {
				for (String s: values880)
					for (String t: valuesMain) {
						StringBuilder sb = new StringBuilder();
						sb.append(removeTrailingPunctuation(s,".,"));
						sb.append(" / ");
						sb.append(t);
						if (! dates.isEmpty()) {
							sb.append(" ");
							sb.append(dates);
						}
						addField(solrFields,"author_display",sb.toString());
					}
			} else {
				for (String s: values880)
					if (dates.isEmpty())
						addField(solrFields,"author_display",s);
					else addField(solrFields,"author_display",s+" "+dates);
				for (String s: valuesMain)
					if (dates.isEmpty())
					addField(solrFields,"author_display",s);
					else addField(solrFields,"author_display",s+" "+dates);
			}
			if (valuesMain.size() > 0) {
				String sort_author = valuesMain.iterator().next();
				if (! dates.isEmpty())
					sort_author += " " + dates;
				addField(solrFields,"author_sort",sort_author);
			}
		}
		
		return solrFields;
	}	

}
