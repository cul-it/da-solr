package edu.cornell.library.integration.support;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class OracleQuery {

    static String DBDriver = "oracle.jdbc.driver.OracleDriver";
    static String DBUrl = "jdbc:oracle:thin:@database.library.cornell.edu:1521:VGER";
    static String DBProtocol = "jdbc:oracle:thin:@";
    static String DBServer = "database.library.cornell.edu:1521:VGER";
    static String DBName = "CORNELLDB";
    static String DBuser = "username";
    static String DBpass = "password";

    static DatabaseMetaData dbmeta = null;


    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn = openConnection(DBDriver, DBProtocol, DBServer, DBName, DBuser, DBpass);
        System.out.println("Got Connection");
        String sql = getReserveItemLookupQuery();
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
           //System.out.println("Equipment");
           while (rs.next()) {
              for (int i=1; i <= mdcolumnCount ; i++) {
                 String colname = rsmd.getColumnName(i);
                 System.out.println(colname+": "+rs.getString(i));
              }
           }

        } catch (SQLException ex) {
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

     public static String getNewLaptopQuery() {
        String sql = ""
        +"SELECT MFHD_ITEM.MFHD_ID, "
        +" MFHD_ITEM.ITEM_ENUM, "
        +" MFHD_ITEM.CHRON, "
        +" CIRC_TRANSACTIONS.CURRENT_DUE_DATE "
        +" FROM MFHD_ITEM LEFT OUTER JOIN "
        +" CIRC_TRANSACTIONS ON MFHD_ITEM.ITEM_ID = CIRC_TRANSACTIONS.ITEM_ID "
        +" WHERE (MFHD_ITEM.MFHD_ID = 7111918)"
        +" ORDER BY MFHD_ITEM.ITEM_ID";
        return sql;
     }

     public static String getOldLaptopQuery() {
         int bibid = 3939392;
        //int bibid = 7111918;"
        String sql = ""
        +"SELECT ITEM.ITEM_ID, "
        +" MFHD_ITEM.ITEM_ENUM, "
        +" BIB_TEXT.TITLE,  "
        +" ITEM_BARCODE.ITEM_BARCODE, "
        +" CIRC_TRANSACTIONS.CURRENT_DUE_DATE, "
        +" MFHD_ITEM.CHRON"
        +" FROM ITEM, BIB_ITEM, BIB_TEXT, MFHD_ITEM, CIRC_TRANSACTIONS, ITEM_BARCODE "
        +" WHERE ITEM.ITEM_ID = BIB_ITEM.ITEM_ID"
        +" AND BIB_ITEM.BIB_ID = BIB_TEXT.BIB_ID"
        +" AND ITEM.ITEM_ID = MFHD_ITEM.ITEM_ID"
        +" AND ITEM.ITEM_ID = ITEM_BARCODE.ITEM_ID"
        +" AND ITEM.ITEM_ID = CIRC_TRANSACTIONS.ITEM_ID (+) "
        +" AND (BIB_TEXT.BIB_ID="+ bibid +")"
        +" ORDER BY MFHD_ITEM.ITEM_ID";
        return sql;
     }
     public static String getReserveItemLookupQuery() {
        String barcode = "31924105764074";
        // String barcode = "31924087325001";
        String sql = ""
        +"SELECT CORNELLDB.CIRC_TRANSACTIONS.CURRENT_DUE_DATE, "
        +" CORNELLDB.LOCATION.LOCATION_DISPLAY_NAME, "
        +" CORNELLDB.ITEM.COPY_NUMBER "
        +" FROM CORNELLDB.ITEM_BARCODE, CORNELLDB.LOCATION,  "
        +" { oj CORNELLDB.ITEM LEFT OUTER JOIN CORNELLDB.CIRC_TRANSACTIONS  "
        +" ON CORNELLDB.ITEM.ITEM_ID = CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID } "
        +" WHERE CORNELLDB.ITEM_BARCODE.ITEM_ID = CORNELLDB.ITEM.ITEM_ID  "
        +" AND CORNELLDB.ITEM.TEMP_LOCATION = CORNELLDB.LOCATION.LOCATION_ID "
        +" AND CORNELLDB.ITEM_BARCODE.ITEM_BARCODE ='31924087325001'";
        return sql;

     }
}
