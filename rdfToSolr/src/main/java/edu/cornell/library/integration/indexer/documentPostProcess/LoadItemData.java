package edu.cornell.library.integration.indexer.documentPostProcess;

import java.io.ByteArrayOutputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

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
		
		
		SolrInputField field = document.getField( "holdings_display" );
		SolrInputField itemField = new SolrInputField("item_record_display");
		for (Object mfhd_id_obj: field.getValues()) {
			String query = "SELECT CORNELLDB.ITEM.* FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM" +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + mfhd_id_obj.toString() + "\'" +
							"AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID";
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
		        	   
		   		  XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
				  ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
				  XMLStreamWriter w = outputFactory.createXMLStreamWriter(xmlstream);
				  w.writeStartDocument("UTF-8", "1.0");
				  w.writeStartElement("record");
				  w.writeStartElement("mfhd_id");
			 	  w.writeCharacters(mfhd_id_obj.toString());
				  w.writeEndElement();

				
	        	  if (debug) 
	        		  System.out.println();
	              for (int i=1; i <= mdcolumnCount ; i++) {
	                 String colname = rsmd.getColumnName(i);
	                 w.writeStartElement(colname.toLowerCase());
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
	                 if (debug)
	               		System.out.println(colname+": "+value);
	                 w.writeCharacters(value);
	                 w.writeEndElement();
	              }
	              w.writeEndElement(); //record
	              w.writeEndDocument();
	              String xml = xmlstream.toString("UTF-8");
	              if (debug)
	            	  System.out.println(xml);
	              itemField.addValue(xml, 1);

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
