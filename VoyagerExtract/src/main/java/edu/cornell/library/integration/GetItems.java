package edu.cornell.library.integration;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.config.IntegrationDataProperties;

public class GetItems {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
   final static Boolean debug = false;

   private IntegrationDataProperties integrationDataProperties;
   

   /**
    * default constructor
    */
   public GetItems() { 
       
   }  
   
   
   /**
    * @return the integrationDataProperties
    */
   public IntegrationDataProperties getIntegrationDataProperties() {
      return this.integrationDataProperties;
   }


   /**
    * @param integrationDataProperties the integrationDataProperties to set
    */
   public void setIntegrationDataProperties(
         IntegrationDataProperties integrationDataProperties) {
      this.integrationDataProperties = integrationDataProperties;
   } 

   


   /**
    * @param args
    */
   public static void main(String[] args) {
     GetItems app = new GetItems();
     app.run();
   }
   
   /**
    * 
    */
   public void run() {
	   Connection voyager = null;
	   Connection mysql = null;
	        
	   String DBDriver = "oracle.jdbc.driver.OracleDriver";
	   String DBProtocol = "jdbc:oracle:thin:@";
	   String DBServer = "database.library.cornell.edu:1521:VGER";
	   String DBUser = "login";
	   String DBPass = "login";

    // actually connect to the database
	   try {

		   Class.forName(DBDriver);
		   Class.forName("com.mysql.jdbc.Driver");
		   
		   String dburl = DBProtocol + DBServer;
		   // System.out.println("database connection url: "+dburl);
		   voyager = DriverManager.getConnection(dburl , DBUser, DBPass);
		   mysql = DriverManager.getConnection("jdbc:mysql://fbw4-dev.library.cornell.edu:3306/item_data","dna","dna password");
		   
		   if (voyager == null) {
			   System.out.println("openconnection: no Oracle connection made");
		   }
		   if (mysql == null) {
			   System.out.println("openconnection: no Mysql connection made");
		   }
	   } catch (SQLException sqlexception) {
		   System.out.println(sqlexception.getMessage());
		   sqlexception.printStackTrace();
	   } catch (Exception exception) {
		   //System.out.println(exception);
		   exception.printStackTrace();
	   }
	   String query = "SELECT CORNELLDB.MFHD_ITEM.*, CORNELLDB.ITEM.*, CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME " +
				" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM, CORNELLDB.ITEM_TYPE" +
				" WHERE CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
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
        	int t = 0;
    		PreparedStatement ins = 
    				mysql.prepareStatement("INSERT INTO item (mfhdid, itemid, json) VALUES (?, ?, ?)");
        	while (rs.next()) {
	        	   
        		ObjectMapper mapper = new ObjectMapper();
        		Map<String,Object> record = new HashMap<String,Object>();
			
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
       				record.put(colname.toLowerCase(), value);
        		}
        		String json = mapper.writeValueAsString(record);
        		ins.setInt(1, Integer.valueOf(record.get("mfhd_id").toString()));
        		ins.setInt(2, Integer.valueOf(record.get("item_id").toString()));
        		ins.setString(3, json);
        		ins.executeUpdate();
        		if (debug)
        			System.out.println(json);
        		t++;
        		if (debug) 
        			if (t >= 10) break;
        	}
        } catch (SQLException ex) {
           System.out.println(query);
           System.out.println(ex.getMessage());
        } catch (JsonProcessingException e) {
        	System.out.println(e.getMessage());
        	e.printStackTrace();
		} catch (IOException e) {
        	System.out.println(e.getMessage());
        	e.printStackTrace();
		} finally {
       
           try {
              if (stmt != null) stmt.close();
              if (rs != null) rs.close();
           } catch (Exception ex) {}
        }
    
	}


private static String convertClobToString(Clob clob) throws SQLException, IOException{
    InputStream inputStream = clob.getAsciiStream();
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, "utf-8");
    return writer.toString();
 }
 

   

}
