package edu.cornell.library.integration.support;

import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.commons.io.IOUtils;


public class OracleQuery {

    static public String DBDriver = "oracle.jdbc.driver.OracleDriver";
    static public String DBUrl = "jdbc:oracle:thin:@database.library.cornell.edu:1521:VGER";
    static public String DBProtocol = "jdbc:oracle:thin:@";
    static public String DBServer = "database.library.cornell.edu:1521:VGER";
    static public String DBName = "CORNELLDB";
    static public String DBuser = "username";
    static public String DBpass = "password";

    static DatabaseMetaData dbmeta = null;


    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn = openConnection(DBDriver, DBProtocol, DBServer, DBName, DBuser, DBpass);
        //System.out.println("Got Connection");
        //String sql = getBibCountQuery();
        //String sql = getBibBlobQuery();
        //String sql = getBibDataQuery();
        //String sql = getOperatorQuery();
        //String sql = getBibHistoryQuery();
        //String sql = getMfhdHistoryQuery();
        //String sql = getSuppressedBibQuery();
        //String sql = getSuppressedBibCountQuery();
        //String sql = getUnSuppressedBibCountQuery();
        //String sql = getSuppressedMfhdQuery();
        //String sql = getSuppressedMfhdCountQuery();
        //String sql = getUnSuppressedMfhdCountQuery();
        //String sql = getActionTypeQuery();
        //String sql = getBibMasterById("8002147"); // 8002147this is suppressed
        String sql = getBibMasterById("5738315"); // 8016993 this one is unsuppressed
        //String sql = getMfhdMasterById("8394936"); //  this one is unsuppressed
        //String sql = getMfhdMasterById("8408371"); // this one is suppressed
        System.out.println(sql);
        queryDatabase(conn, sql);

        closeConnection(conn);
    }

    public static Connection openConnection(String driver, String databaseProtocol, String databaseServer, String databaseName, String databaseUsername, String databasePassword) {


        Connection connection = null;

        // actually connect to the database
        try {

           Class.forName(driver);
           String dburl = databaseProtocol + databaseServer;
           // System.out.println("database connection url: "+dburl);
           connection = DriverManager.getConnection(dburl , databaseUsername, databasePassword);

           if (connection == null) {
              System.out.println("openconnection: no connection made");
           }
           // end alert if no connection made
        } catch (SQLException sqlexception) {
           System.out.println(sqlexception.getMessage());
           sqlexception.printStackTrace();
        } catch (Exception exception) {
           //System.out.println(exception);
           exception.printStackTrace();
        }

        return connection;
     }


     public static void queryDatabase(Connection conn, String sql) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        try {
           stmt = conn.createStatement();

           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           while (rs.next()) {
              System.out.println();
              for (int i=1; i <= mdcolumnCount ; i++) {
                 String colname = rsmd.getColumnName(i);
                 int coltype = rsmd.getColumnType(i);
                 if (coltype == java.sql.Types.CLOB) {
                    Clob clob = rs.getClob(i);                     
                    System.out.println(colname+": "+ convertClobToString(clob));
                 } else {                 
                    System.out.println(colname+": "+rs.getString(i));
                 }
              }
           }

        } catch (SQLException ex) {
           System.out.println(sql);
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




     public static void closeConnection(Connection conn) {
        if (conn != null) {
          try {
            conn.close();
          }
          catch (SQLException SQLEx) { /* ignore */ }
        }

     }
     
     public static String convertClobToString(Clob clob) throws Exception {
        InputStream inputStream = clob.getAsciiStream();
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "utf-8");
        return writer.toString();
     }
     
     

     public static String getLocationsQuery() {
        String sql = ""
        +"SELECT "
        +" LOCATION.LOCATION_CODE, "
        +" LOCATION.LOCATION_ID, "
        +" LOCATION.SUPPRESS_IN_OPAC, "
        +" LOCATION.MFHD_COUNT, "
        +" LOCATION.LOCATION_OPAC, "
        +" LOCATION.LOCATION_NAME, "
        +" LOCATION.LOCATION_SPINE_LABEL, "
        +" LOCATION.LOCATION_DISPLAY_NAME, "
        +" LOCATION.LIBRARY_ID, "
        +" LIBRARY.LIBRARY_NAME, "
        +" LIBRARY.LIBRARY_DISPLAY_NAME  "
        +" FROM LOCATION LEFT OUTER JOIN "
        +" LIBRARY ON LOCATION.LIBRARY_ID = LIBRARY.LIBRARY_ID ";
        return sql;
     }

     public static String getLibrariesQuery() {
        String sql = ""
        +"SELECT *"
        +" FROM LIBRARY";
        return sql;
     }
     
     public static String getBibHistoryQuery() {
        // ACTION_DATE: 2012-11-12 12:57:16.0
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        Calendar earlier = now;
        earlier.add(Calendar.HOUR, -24);
        String ds = df.format(earlier.getTime());
        
        String sql = ""
              +" SELECT * FROM CORNELLDB.BIB_HISTORY"
              +" WHERE SUPPRESS_IN_OPAC = 'N'"
              +" AND to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'";  
              return sql;   
      
     }
     
     public static String getSuppressedBibQuery() {
        // ACTION_DATE: 2012-11-12 12:57:16.0
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        Calendar earlier = now;
        earlier.add(Calendar.HOUR, -24);
        String ds = df.format(earlier.getTime());
        System.out.println("ds: "+ds);
        String sql = ""
              +" SELECT * FROM CORNELLDB.BIB_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'Y'"
              +" AND to_char(BIB_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'";  
        return sql;   
      
     }
     
     public static String getSuppressedBibCountQuery() {
   
        String sql = ""
              +" SELECT COUNT(BIB_ID) FROM CORNELLDB.BIB_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'Y'"; 
        return sql;   
      
     }
     
     public static String getUnSuppressedBibCountQuery() {
        
        String sql = ""
              +" SELECT COUNT(BIB_ID) FROM CORNELLDB.BIB_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'N'"; 
        return sql;   
      
     }
     
     public static String getBibMasterById(String bibid) {        
        String sql = ""
              +" SELECT * FROM CORNELLDB.BIB_MASTER"
              +" WHERE BIB_ID ='"+ bibid +"'"; 
        return sql;  
      
     }
     
     public static String getMfhdMasterById(String mfhdid) {        
        String sql = ""
              +" SELECT * FROM CORNELLDB.MFHD_MASTER"
              +" WHERE MFHD_ID ='"+ mfhdid +"'"; 
        return sql;  
      
     }
     
     public static String getMfhdHistoryQuery() {
        // ACTION_DATE: 2012-11-12 12:57:16.0
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        Calendar earlier = now;
        earlier.add(Calendar.HOUR, -24);
        String ds = df.format(earlier.getTime());
        
        String sql = ""
              +" SELECT * FROM CORNELLDB.MFHD_HISTORY"
              +" WHERE SUPPRESS_IN_OPAC = 'N'"
              +" AND to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'";  
              return sql;   
      
     }
     
      
     
     public static String getSuppressedMfhdQuery() {
        // ACTION_DATE: 2012-11-12 12:57:16.0
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        Calendar earlier = now;
        earlier.add(Calendar.HOUR, -24);
        String ds = df.format(earlier.getTime());
        System.out.println("ds: "+ds);
        String sql = ""
              +" SELECT * FROM CORNELLDB.MFHD_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'Y'"
              +" AND to_char(MFHD_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'";  
              return sql;   
      
     }
     
     public static String getSuppressedMfhdCountQuery() {
         
        String sql = ""
              +" SELECT COUNT(MFHD_ID) FROM CORNELLDB.MFHD_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'Y'";  
        return sql;   
      
     }
     
     public static String getUnSuppressedMfhdCountQuery() {
        
        String sql = ""
              +" SELECT COUNT(MFHD_ID) FROM CORNELLDB.MFHD_MASTER"
              +" WHERE SUPPRESS_IN_OPAC = 'N'";  
        return sql;   
      
     }
     
     public static String getBibCountQuery() {
        // ACTION_DATE: 2012-11-12 12:57:16.0
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar now = Calendar.getInstance();
        Calendar earlier = now;
        earlier.add(Calendar.HOUR, -3);
        String ds = df.format(earlier.getTime());
        
        String sql = ""
              +" SELECT COUNT(BIB_MASTER.BIB_ID) FROM BIB_MASTER"
              +"  WHERE to_char(BIB_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'";  
              return sql;   
      
     }
     
     
     
   public static String getActionTypeQuery() {
      String sql = ""
         +"SELECT * FROM CORNELLDB.ACTION_TYPE";
      return sql;
   }

   public static String getOperatorQuery() {
      String sql = ""
         +"SELECT OPERATOR_ID, FIRST_NAME, LAST_NAME FROM CORNELLDB.OPERATOR";
      return sql;
   }
 
   public static String getBibDataQuery() {
      int bibid = 5430046;
      String sql = ""
         +"SELECT BIB_ID,SEQNUM,RECORD_SEGMENT FROM CORNELLDB.BIB_DATA WHERE BIB_ID = "+ bibid + " ORDER BY SEQNUM";
      return sql;
   }
 
   public static String getBibBlobQuery() {
      String sql = ""
         +"SELECT BIB_ID,MARC_RECORD FROM CORNELLDB.BIBBLOB_VW "
         +" WHERE ROWNUM < 20 ";
      return sql;
   }
   
}