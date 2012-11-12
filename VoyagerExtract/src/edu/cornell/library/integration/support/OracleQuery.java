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
        String sql = getLocationsQuery();
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
     }}
