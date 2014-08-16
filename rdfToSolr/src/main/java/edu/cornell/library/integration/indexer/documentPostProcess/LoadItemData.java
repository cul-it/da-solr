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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.resultSetToFields.NameFieldsAsColumnsRSTF;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/** If holdings record ID(s) are identified in holdings_display, pull any matching
 *  item records from the voyager database and serialize them in json.
 *  */
public class LoadItemData implements DocumentPostProcess{

	final static Boolean debug = false;
	Map<Integer,Location> locations = new HashMap<Integer,Location>();
	
	@Override
	public void p(String recordURI, RDFService mainStore,
			RDFService localStore, SolrInputDocument document, Connection conn) throws Exception {

		Boolean multivol = false;
		SolrInputField multivolField = new SolrInputField("multivol_b");
		Collection<String> locsAndCopyNums = new HashSet<String>();
		
		if (! document.containsKey("holdings_display")) {
			multivolField.setValue(multivol, 1.0f);
			document.put("multivol_b", multivolField);
			return;
		}
		
		SolrInputField field = document.getField( "holdings_display" );
		SolrInputField itemField = new SolrInputField("item_record_display");
		SolrInputField itemlist = new SolrInputField("item_display");
		for (Object mfhd_id_obj: field.getValues()) {
			if (debug)
				System.out.println(mfhd_id_obj.toString());
			String query = "SELECT CORNELLDB.MFHD_ITEM.*, CORNELLDB.ITEM.*, CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME, CORNELLDB.ITEM_BARCODE.ITEM_BARCODE " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM_TYPE, CORNELLDB.ITEM" +
					" LEFT OUTER JOIN CORNELLDB.ITEM_BARCODE ON CORNELLDB.ITEM_BARCODE.ITEM_ID = CORNELLDB.ITEM.ITEM_ID " +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + mfhd_id_obj.toString() + "\'" +
					   " AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
					   " AND CORNELLDB.ITEM.ITEM_TYPE_ID = CORNELLDB.ITEM_TYPE.ITEM_TYPE_ID" +
					   " AND CORNELLDB.ITEM_BARCODE.BARCODE_STATUS = '1'";
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
		        	   
	        		ObjectMapper mapper = new ObjectMapper();
	        		Map<String,Object> record = new HashMap<String,Object>();
	        		record.put("mfhd_id",mfhd_id_obj.toString());
	        		int copyNumber = 0;
	        		int locId = 0;
				
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
	       					value = "";
	       				if ((colname.equalsIgnoreCase("temp_location")
	       						|| colname.equalsIgnoreCase("perm_location"))
	       						&& ! value.equals("0")) {
	       					if ((locId == 0) || colname.equalsIgnoreCase("temp_location")) {
	       						locId = Integer.valueOf(value);
	       					}
	       					if (debug)
	       						System.out.println(colname+": "+value);
	       					Location l = getLocation(recordURI,mainStore,localStore,Integer.valueOf(value));
	       					record.put(colname.toLowerCase(), l);
	       				} else {
	       					record.put(colname.toLowerCase(), value);
	       					if (debug)
	       						System.out.println(colname+": "+value);
	       				}
	       				
	       				if (colname.equalsIgnoreCase("copy_number")) {
	       					copyNumber = Integer.valueOf(value);
	       				}
		        		
		        		if (! multivol && colname.equalsIgnoreCase("item_enum")) {
		        			String[] enums = value.split("[:;]\\s*");
		        			for (String s : enums) {
			        			if (debug) System.out.println("evaluating enum for multivol: "+s);
		        				if (s.startsWith("v.")
		        						|| s.startsWith("no.")
		        						|| s.startsWith("th.")
		        						|| s.startsWith("t.")
		        						|| s.endsWith(" ser.")
		        						|| s.endsWith(" ed."))
		        					multivol = true;
		        			}
		        		}

	        		}
	        		
	        		String json = mapper.writeValueAsString(record);
	        		if (debug)
	        			System.out.println(json);
	        		itemField.addValue(json, 1);
	        		
	        		String locAndCopyNum = locId + "+" + copyNumber;
	        		if (locsAndCopyNums.contains(locAndCopyNum)) 
	        			multivol = true;
	        		else
	        			locsAndCopyNums.add(locAndCopyNum);
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
		multivolField.setValue(multivol, 1.0f);
		document.put("multivol_b", multivolField);
		document.put("item_display", itemlist);
		document.put("item_record_display", itemField);
	}
	
	private Location getLocation (String recordURI, RDFService mainStore,
			RDFService localStore, Integer id) throws Exception{

		if (locations.containsKey(id))
			return locations.get(id);
		
		String loc_uri = "<http://da-rdf.library.cornell.edu/individual/loc_"+id+">";
		if (debug)
			System.out.println(loc_uri);
		SPARQLFieldMakerImpl fm = new SPARQLFieldMakerImpl().
        setName("location").
        addMainStoreQuery("location",
		   	"SELECT DISTINCT ?name ?library ?code \n"+
		   	"WHERE {\n"+
		    "  "+loc_uri+" rdf:type intlayer:Location.\n" +
		    "  "+loc_uri+" intlayer:code ?code.\n" +
		    "  "+loc_uri+" rdfs:label ?name.\n" +
		    "  OPTIONAL {\n" +
		    "    "+loc_uri+" intlayer:hasLibrary ?liburi.\n" +
		    "    ?liburi rdfs:label ?library.\n" +
		    "}}").
        addResultSetToFields( new NameFieldsAsColumnsRSTF());
		Map<? extends String, ? extends SolrInputField> tempfields = 
				fm.buildFields(recordURI, mainStore, localStore);
		if (debug) {
			System.out.println(tempfields.toString());
			System.out.println(tempfields.get("name").getValue());
		}
		Location l = new Location();
		l.code = tempfields.get("code").getValue().toString();
		l.name = tempfields.get("name").getValue().toString();
		l.library = tempfields.get("library").getValue().toString();
		l.number = id;
		locations.put(id, l);
		return l;
	}
	
    private static String convertClobToString(Clob clob) throws Exception {
        InputStream inputStream = clob.getAsciiStream();
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "utf-8");
        return writer.toString();
     }
	
	private static class Location {
		public String code;
		public Integer number;
		public String name;
		public String library;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("code: ");
			sb.append(this.code);
			sb.append("; number: ");
			sb.append(this.number);
			sb.append("; name: ");
			sb.append(this.name);
			sb.append("; library: ");
			sb.append(this.library);
			return sb.toString();
		}
	}


}
