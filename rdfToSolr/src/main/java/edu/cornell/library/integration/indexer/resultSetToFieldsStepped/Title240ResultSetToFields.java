package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class Title240ResultSetToFields implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		MarcRecord rec = new MarcRecord();
		MarcRecord mainEntryRec = new MarcRecord();
		String mainEntry = null;
		String mainEntryVern = null;

		for( String resultKey: results.keySet()){
			if (resultKey.equals("title_240"))
				rec.addDataFieldResultSet(results.get(resultKey));
			else 
				mainEntryRec.addDataFieldResultSet(results.get(resultKey));

		}

		// Process mainEntryRec to identify author metadata for click-to-search
		Map<Integer,FieldSet> sortedFields = mainEntryRec.matchAndSortDataFields();
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			for (DataField f: dataFields) {
				if (f.tag.equals("880"))
					mainEntryVern = removeTrailingPunctuation(f.concateSubfieldsOtherThan6(),".,");
				else
					mainEntry = removeTrailingPunctuation(f.concateSubfieldsOtherThan6(),".,");
			}
		}

		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		sortedFields = rec.matchAndSortDataFields();
		ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Set<String> values880 = new HashSet<String>();
			Set<String> valuesMain = new HashSet<String>();
			for (DataField f: dataFields) {
				String field = removeTrailingPunctuation(f.concatenateSpecificSubfields("adfgklmnoprs"),".,");
				String cts = removeTrailingPunctuation(f.concatenateSpecificSubfields("adfgklmnoprs"),".,");
				if (cts.length() > 0) {
					field += "|"+cts;
				}
				if (f.tag.equals("880")) {
					if (mainEntryVern != null)
						field += "|"+mainEntryVern;
					else if (mainEntry != null)
						field += "|"+mainEntry;
					values880.add(field);
				} else {
					if (mainEntry != null)
						field += "|"+mainEntry;
					valuesMain.add(field);
				}
			}
			for (String s: values880)
				addField(solrFields,"title_uniform_display",s);	
			for (String s: valuesMain)
				addField(solrFields,"title_uniform_display",s);	
		}

		step.setFields(solrFields);
		return step;

	
	}
		
}
