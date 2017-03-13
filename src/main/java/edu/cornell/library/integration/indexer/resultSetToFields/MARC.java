package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;

/**
 * processing all bibliographic MARC data into a MARC XML blob to insert into Solr doc
 * 
 */
public class MARC implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();		

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
			rec.addControlFieldResultSet(marc_control_fields,true);
		}
		
		if (results.containsKey("marc_data_fields")) {
			ResultSet marc_data_fields = results.get("marc_data_fields");
			rec.addDataFieldResultSet( marc_data_fields );
		}
		addField(fields, "marc_display", rec.toString("xml"));
		addField(fields, "id", rec.id);
		if (rec.modified_date != null)
			addField(fields, "bibid_display", rec.id+"|"+rec.modified_date.substring(0, 14));
		else
			addField(fields, "bibid_display", rec.id);
		
		return fields;
	}	

}
