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
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class PubInfoResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			rec.addDataFieldResultSet(rs);
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
			String publisher = null;
			String pubplace = null;
			String publisherVern = null;
			String pubplaceVern = null;
			for (DataField f: dataFields) {
				if (relation == null) {
					if (f.ind2.equals('0')) relation = "pub_prod";
					else if (f.ind2.equals('1')) relation = "pub_info";
					else if (f.ind2.equals('2')) relation = "pub_dist";
					else if (f.ind2.equals('3')) relation = "pub_manu";
					else if (f.ind2.equals('4')) relation = "pub_copy";
				}
				if (f.tag.equals("880")) {
					values880.add(f.concateSubfieldsOtherThan6());
					if (relation.equals("pub_info")) {
						publisherVern = f.concatenateSpecificSubfields("b");
						pubplaceVern = f.concatenateSpecificSubfields("a");
					}
				} else {
					valuesMain.add(f.concateSubfieldsOtherThan6());
					if (relation.equals("pub_info")) {
						publisher = f.concatenateSpecificSubfields("b");
						pubplace = f.concatenateSpecificSubfields("a");
					}
				}
			}
			if (relation != null) {
				for (String s: values880)
					addField(solrFields,relation+"_display",s);	
				for (String s: valuesMain)
					addField(solrFields,relation+"_display",s);
			}
			if (pubplace != null) {
				if (pubplaceVern != null)
					addField(solrFields,"pubplace_display",pubplaceVern +" / "+ pubplace);
				else
					addField(solrFields,"pubplace_display",pubplace);
			} else if (pubplaceVern != null)
				addField(solrFields,"pubplace_display",pubplaceVern);

			if (publisher != null) {
				if (publisherVern != null)
					addField(solrFields,"publisher_display",publisherVern +" / "+ publisher);
				else
					addField(solrFields,"publisher_display",publisher);
			} else if (publisherVern != null)
				addField(solrFields,"publisher_display",publisherVern);
		}
				
		return solrFields;
	}	
}
