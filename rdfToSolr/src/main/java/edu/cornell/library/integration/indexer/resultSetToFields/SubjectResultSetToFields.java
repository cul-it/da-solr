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
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class SubjectResultSetToFields implements ResultSetToFields {

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
		
			String main_fields = "", dashed_fields = "";
			for (DataField f: dataFields) {
				
				if (f.mainTag.equals("600")) {
					main_fields = "abcdefghkjlmnopqrstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("610")) {
					main_fields = "abcdefghklmnoprstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("611")) {
					main_fields = "acdefghklnpqstu";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("630")) {
					main_fields = "adfghklmnoprst";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("648")) {
					main_fields = "a";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("650")) {
					main_fields = "abcd";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("651")) {
					main_fields = "a";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("653")) {
					main_fields = "a";
				} else if (f.mainTag.equals("654")) {
					main_fields = "abe";
					dashed_fields = "vyz";
				} else if (f.mainTag.equals("655")) {
					main_fields = "ab";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("656")) {
					main_fields = "ak";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("657")) {
					main_fields = "a";
					dashed_fields = "vxyz";
				} else if (f.mainTag.equals("658")) {
					main_fields = "abcd";
				} else if (f.mainTag.equals("662")) {
					main_fields = "abcdfgh";
				} else if (f.mainTag.equals("690")) {
					main_fields = "abvxyz";
				} else if (f.mainTag.equals("691")) {
					main_fields = "abvxyz";
				} else if ((f.mainTag.equals("692")) || (f.mainTag.equals("693")) ||
						(f.mainTag.equals("694")) || (f.mainTag.equals("695")) ||
						(f.mainTag.equals("696")) || (f.mainTag.equals("697")) ||
						(f.mainTag.equals("698")) || (f.mainTag.equals("699"))) {
					main_fields = "a";
				}
				if (! main_fields.equals("")) {
					StringBuilder sb = new StringBuilder();
					sb.append(f.concatenateSpecificSubfields(main_fields));
					String dashed_terms = f.concatenateSpecificSubfields("|",dashed_fields);
					if ((dashed_terms != null) && ! dashed_terms.equals("")) {
						sb.append("|"+dashed_terms);
					}
					if (f.tag.equals("880")) {
						values880.add(removeTrailingPunctuation(sb.toString(),"."));
					} else {
						valuesMain.add(removeTrailingPunctuation(sb.toString(),"."));
					}
				}
			}
			for (String s: values880)
				addField(solrFields,"subject_display",s);
			for (String s: valuesMain)
				addField(solrFields,"subject_display",s);
		}
		
		return solrFields;
	}	

}
