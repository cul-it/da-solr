package edu.cornell.library.integration.indexer.resultSetToFields;

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
 * processing into contents_display and partial_contents_display
 * 
 */
public class TOCResultSetToFields implements ResultSetToFields {

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
			String relation = null;
			String subfields = "atr";
			for (DataField f: dataFields) {
				if(relation == null) {
					if (f.ind1.equals('1') || f.ind1.equals('8')) {
						relation = "contents";
					} else if (f.ind1.equals('0')) {
						relation = "contents";
						subfields = "agtr";
					} else if (f.ind1.equals('2')) {
						relation = "partial_contents";
					}
				}
				if (f.tag.equals("880"))
					values880.add(f.concateSpecificSubfields(subfields));
				else
					valuesMain.add(f.concateSpecificSubfields(subfields));
			}
			if (relation != null) {
				for (String s: values880)
					if (values880.size() == 1) {
						for(String item: s.split(" *-- *"))
							addField(solrFields,relation+"_display",item);
					} else {
						addField(solrFields,relation+"_display",s);
					}
				for (String s: valuesMain)
					if (valuesMain.size() == 1) {
						for (String item: s.split(" *-- *"))
							addField(solrFields,relation+"_display",item);
					} else {
						addField(solrFields,relation+"_display",s);
					}
			}
		}
		
		return solrFields;
	}	

}
