package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class MARCResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();		

		MarcRecord rec = new MarcRecord();
				
		if (results.containsKey("marc_leader")) {
			ResultSet marc_leader = results.get("marc_leader");
			if( marc_leader != null && marc_leader.hasNext() ){
				QuerySolution sol = marc_leader.nextSolution();
				rec.leader = nodeToString( sol.get("l"));
			}
		} 		
		if( rec.leader == null || rec.leader.trim().isEmpty()){
			throw new Error("Leader should NEVER be missing from a MARC record.");			
		}
		
		if (results.containsKey("marc_control_fields")) {
			ResultSet marc_control_fields = results.get("marc_control_fields");
			rec.addControlFieldResultSet(marc_control_fields);
		}
		
		if (results.containsKey("marc_data_fields")) {
			ResultSet marc_data_fields = results.get("marc_data_fields");
			rec.addDataFieldResultSet( marc_data_fields );
		}
		addField(fields, "marc_display", rec.toString("xml"));
		
		return fields;
	}	

}
