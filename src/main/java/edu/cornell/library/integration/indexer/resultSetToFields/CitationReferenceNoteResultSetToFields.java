package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

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
 * processing 510 notes into references_display, indexed_by_display, 
 * indexed_in_its_entirety_by_display, and indexed_selectively_by_display
 * 
 */
public class CitationReferenceNoteResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
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
			for (DataField f: dataFields) {
				if (relation == null) {
					if (f.ind1.equals('4')) relation = "references";
					else if (f.ind1.equals('3')) relation = "references";
					else if (f.ind1.equals(' ')) relation = "references";
					else if (f.ind1.equals('2')) relation = "indexed_selectively_by";
					else if (f.ind1.equals('1')) relation = "indexed_in_its_entirety_by";
					else if (f.ind1.equals('0')) relation = "indexed_by";
				}
				if (f.tag.equals("880"))
					values880.add(f.concatenateSpecificSubfields("abcux3"));
				else
					valuesMain.add(f.concatenateSpecificSubfields("abcux3"));
			}
			if (relation != null) {
				for (String s: values880)
					addField(solrFields,relation+"_display",s);	
				for (String s: valuesMain)
					addField(solrFields,relation+"_display",s);	
			}
		}
				
		return solrFields;
	}	
}
