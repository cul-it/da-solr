package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import static edu.cornell.library.integration.indexer.IndexingUtilities.prettyFormat;
import edu.cornell.library.integration.indexer.MarcRecord.*;

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
			while (marc_control_fields.hasNext()) {
				QuerySolution sol = marc_control_fields.nextSolution();
				String f_uri = nodeToString( sol.get("f") );
				Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
				ControlField f = new ControlField();
				f.tag = nodeToString(sol.get("t"));
				f.value = nodeToString(sol.get("v"));
				f.id = field_no;
				rec.control_fields.put(field_no, f);
			}
		}
		
		if (results.containsKey("marc_data_fields")) {
			ResultSet marc_data_fields = results.get("marc_data_fields");
			DataField f = new DataField();
			f.id = 0;
			while (marc_data_fields.hasNext()) {
				QuerySolution sol = marc_data_fields.nextSolution();
				String f_uri = nodeToString( sol.get("f") );
				Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
				if (f.id != field_no) {
					if (f.id > 0) {
						rec.data_fields.put(f.id, f);
					}
					f = new DataField();
					f.id = field_no;
					f.tag = nodeToString(sol.get("t"));
					f.ind1 = nodeToString(sol.get("i1")).charAt(0);
					f.ind2 = nodeToString(sol.get("i2")).charAt(0);
				}

				Subfield sf = new Subfield();
				String sf_uri = nodeToString( sol.get("sf"));
				Integer subfield_no = Integer.valueOf( sf_uri.substring( sf_uri.lastIndexOf('_') + 1 ) );
				sf.id = subfield_no;
				sf.code = nodeToString(sol.get("c")).charAt(0);
				sf.value = nodeToString(sol.get("v"));
				f.subfields.put(sf.id, sf);
			}
			rec.data_fields.put(f.id, f);
		}		
		addField(fields, "marc_display", rec.toString("xml"));
		
		return fields;
	}	

}
