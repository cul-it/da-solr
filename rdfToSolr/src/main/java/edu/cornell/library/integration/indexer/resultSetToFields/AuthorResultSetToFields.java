package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.getSortHeading;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

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
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
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
			Set<String> valuesFacet = new HashSet<String>();
			String dates = "";
			String cts = "";
			String cts880 = "";
			String mainTag = null;
		
			for (DataField f: dataFields) {
				mainTag = f.mainTag;
				String subfields;
				String ctsSubfields;
				String facetOrFileSubfields;
				if (mainTag.equals("100")) {
					subfields = "abcq";
					ctsSubfields = "abcdq";
					facetOrFileSubfields = "abcdq";
				} else {
					subfields = "abcdefghijklmnopqrstuvwxyz";
					ctsSubfields = "ab";
					facetOrFileSubfields = "abcdefghijklmnopqrstuvwxyz";
				}
				String value = f.concatenateSpecificSubfields(subfields);
				if ( ! value.isEmpty() ) {
					if (f.tag.equals("880")) {
						values880.add(value);
						cts880 = f.concatenateSpecificSubfields(ctsSubfields);
					} else {
						if (mainTag.equals("100"))
							dates = removeTrailingPunctuation(f.concatenateSpecificSubfields("d"),".,");
						valuesMain.add(value);
						cts = f.concatenateSpecificSubfields(ctsSubfields);
					}
				}
				value = f.concatenateSpecificSubfields(facetOrFileSubfields);
				if ( ! value.isEmpty() )
					valuesFacet.add(value);
			}
			if ((values880.size() == 1) && (valuesMain.size() == 1 )) {
				for (String s: values880)
					for (String t: valuesMain) {
						StringBuilder sb_piped = new StringBuilder();
						StringBuilder sb_disp = new StringBuilder();
						String cleaned_s = removeTrailingPunctuation(s,".,");
						sb_piped.append(cleaned_s);
						sb_disp.append(cleaned_s);
						sb_piped.append("|");
						sb_piped.append(cts880);
						sb_piped.append("|");
						sb_disp.append(" / ");
						sb_piped.append(t);
						sb_disp.append(t);
						if (! dates.isEmpty()) {
							sb_piped.append(" ");
							sb_piped.append(dates);
							sb_disp.append(" ");
							sb_disp.append(dates);
						}
						sb_piped.append("|");
						sb_piped.append(cts);
						String author_display = sb_disp.toString();
						addField(solrFields,"author_display",removeTrailingPunctuation(author_display,", "));
						addField(solrFields,"author_cts",sb_piped.toString());
					}
			} else {
				for (String s: values880)
					if (dates.isEmpty()) {
						addField(solrFields,"author_cts",s+"|"+cts880);
						addField(solrFields,"author_display",removeTrailingPunctuation(s,", "));
					} else {
						addField(solrFields,"author_cts",s+" "+dates+"|"+cts880);
						addField(solrFields,"author_display",removeTrailingPunctuation(s+" "+dates,", "));
					}
				for (String s: valuesMain)
					if (dates.isEmpty()) {
						addField(solrFields,"author_cts",s+"|"+cts);
						addField(solrFields,"author_display",removeTrailingPunctuation(s,", "));
					} else {
						addField(solrFields,"author_cts",s+" "+dates+"|"+cts);
						addField(solrFields,"author_display",removeTrailingPunctuation(s+" "+dates,", "));
					}
			}
			for (String s : valuesFacet) {
				addField(solrFields,"author_"+mainTag+"_filing",getSortHeading(s));
				addField(solrFields,"author_facet",removeTrailingPunctuation(s,"., "));
			}
				
			if (valuesMain.size() > 0) {
				String sort_author = getSortHeading(valuesMain.iterator().next());
				if (! dates.isEmpty())
					sort_author += " " + dates;
				addField(solrFields,"author_sort",sort_author);
			}
		}
		
		return solrFields;
	}	

}
