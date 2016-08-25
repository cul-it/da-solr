package edu.cornell.library.integration.indexer.documentPostProcess;

import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** Pull any barcodes from the item_barcode table, and populate them into
 * barcode_t alongside any values that might already be there (from the 903‡p).
 */
public class BarcodeSearch implements DocumentPostProcess{

	final static Boolean debug = false;
	Collection<String> barcodes = new HashSet<String>();
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {
		
		if (! document.containsKey("holdings_display")) {
			return;
		}
		
		try (  Connection conn = config.getDatabaseConnection("Voy")  ) {
			generateFields(document,conn);
		}
	}
	
    private void generateFields(SolrInputDocument document,Connection conn) {
		
		SolrInputField field = document.getField( "holdings_display" );
		for (Object mfhd_id_obj: field.getValues()) {
			String mfhd_id = mfhd_id_obj.toString();
			if (mfhd_id.contains("|")) {
				mfhd_id = mfhd_id.substring(0, mfhd_id.indexOf('|'));
			}
			if (debug)
				System.out.println(mfhd_id);
			String query = "SELECT CORNELLDB.ITEM_BARCODE.ITEM_BARCODE " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM_BARCODE " +
					" WHERE CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM_BARCODE.ITEM_ID " +
					"  AND CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + mfhd_id + "\'";
			if (debug)
				System.out.println(query);
	
	        Statement stmt = null;
	        ResultSet rs = null;
	        ResultSetMetaData rsmd = null;
	        try {
	        	stmt = conn.createStatement();
	
	        	rs = stmt.executeQuery(query);
	        	rsmd = rs.getMetaData();
	        	int mdcolumnCount = rsmd.getColumnCount();
	        	while (rs.next()) {

	        		if (debug) 
	        			System.out.println();
	        		for (int i=1; i <= mdcolumnCount ; i++) {
	        			String colname = rsmd.getColumnName(i);
	        			int coltype = rsmd.getColumnType(i);
	        			String value = null;
	       				if (coltype == java.sql.Types.CLOB) {
	       					Clob clob = rs.getClob(i);  
	        				value = convertClobToString(clob);
	        			} else { 
	        				value = rs.getString(i);
	       				}
	       				if (value == null)
	       					continue;
	       				if (colname.equalsIgnoreCase("item_barcode")) {
	       					if (debug) System.out.println("Found barcode associated with item: "+value);
	       					barcodes.add(value);
	       				}
	        		}
	        	}
	        } catch (SQLException ex) {
	           System.out.println(query);
	           System.out.println(ex.getMessage());
	        } catch (Exception ex) {
	        	ex.printStackTrace();
	        } finally {
	       
	           try {
	              if (stmt != null) stmt.close();
	              if (rs != null) rs.close();
	           } catch (Exception ex) {}
	        }
        
		}

		if (barcodes.size() == 0)
			return;
		if (document.containsKey("barcode_t")) {
			SolrInputField f = document.getField("barcode_t");
			for (Object barcode : f.getValues())
				barcodes.add(barcode.toString());
		}
		SolrInputField newF = new SolrInputField("barcode_t");
		for (String barcode : barcodes) {
			newF.addValue(barcode, 1.0f);
		}
		document.put("barcode_t",newF);
	}

	private static String convertClobToString(Clob clob) throws Exception {
        StringWriter writer = new StringWriter();
        try (  InputStream inputStream = clob.getAsciiStream() ){
        	IOUtils.copy(inputStream, writer, "utf-8"); }
        return writer.toString();
     }

}
