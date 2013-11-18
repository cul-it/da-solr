package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.codehaus.jackson.map.ObjectMapper;

import edu.cornell.library.integration.support.OracleQuery;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/** To boost shadow records, identify them, then set boost to X times current boost.
 *  We're currently boosting the whole record, but we may want to put a special boost
 *  on the title in the future to promote title searches.
 *  */
public class LoadItemData implements DocumentPostProcess{

	final static Boolean debug = false;
	
	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection voyager) throws Exception {

		if (! document.containsKey("holdings_display"))
			return;
		
		SolrInputField field = document.getField( "holdings_display" );
		SolrInputField itemField = new SolrInputField("item_record_display");
		for (Object mfhd_id_obj: field.getValues()) {
			String query = "SELECT CORNELLDB.MFHD_ITEM.*, CORNELLDB.ITEM.*, CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM, CORNELLDB.ITEM_TYPE" +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + mfhd_id_obj.toString() + "\'" +
					   " AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
					   " AND CORNELLDB.ITEM.ITEM_TYPE_ID = CORNELLDB.ITEM_TYPE.ITEM_TYPE_ID";
			if (debug)
				System.out.println(query);
	
	        Statement stmt = null;
	        ResultSet rs = null;
	        ResultSetMetaData rsmd = null;
	        try {
	        	stmt = voyager.createStatement();
	
	        	rs = stmt.executeQuery(query);
	        	rsmd = rs.getMetaData();
	        	int mdcolumnCount = rsmd.getColumnCount();
	        	while (rs.next()) {
		        	   
	        		ObjectMapper mapper = new ObjectMapper();
	        		Map<String,String> record = new HashMap<String,String>();
	        		record.put("mfhd_id",mfhd_id_obj.toString());
				
	        		if (debug) 
	        			System.out.println();
	        		for (int i=1; i <= mdcolumnCount ; i++) {
	        			String colname = rsmd.getColumnName(i);
	        			int coltype = rsmd.getColumnType(i);
	        			String value = null;
	       				if (coltype == java.sql.Types.CLOB) {
	       					Clob clob = rs.getClob(i);  
	        				value = OracleQuery.convertClobToString(clob);
	        			} else { 
	        				value = rs.getString(i);
	       				}
	       				if (value == null)
	       					value = "";
	       				
	       				record.put(colname.toLowerCase(), value);
	        		}
	        		
	        		String json = mapper.writeValueAsString(record);
	        		if (debug)
	        			System.out.println(json);
	        		itemField.addValue(json, 1);
	        	}
	        } catch (SQLException ex) {
	           System.out.println(query);
	           System.out.println(ex.getMessage());
	        } catch (Exception ex) {
	           System.out.println(ex.getMessage());   
	        } finally {
	       
	           try {
	              if (stmt != null) stmt.close();
	              if (rs != null) rs.close();
	           } catch (Exception ex) {}
	        }
	        System.out.println();
        
		}
		document.put("item_record_display", itemField);
	}

}
