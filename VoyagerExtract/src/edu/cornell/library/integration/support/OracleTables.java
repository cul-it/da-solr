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


public class OracleTables {

    static String DBDriver = "oracle.jdbc.driver.OracleDriver";
    static String DBUrl = "jdbc:oracle:thin:@database.library.cornell.edu:1521:VGER";
    static String DBProtocol = "jdbc:oracle:thin:@";
    static String DBServer = "database.library.cornell.edu:1521:VGER";
    static String DBName = "CORNELLDB";
    static String DBuser = "login";
    static String DBpass = "login";

    static DatabaseMetaData dbmeta = null;


    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn = openConnection(DBDriver, DBProtocol, DBServer, DBName, DBuser, DBpass);
        System.out.println("Got Connection");
        List<String> tables = getTableNames(conn);
        for (int i=0; i < tables.size(); i++) {
            System.out.println(tables.get(i));
            describeTable(conn, tables.get(i));
        }

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


     public static List<String> getTableNames(Connection conn) {
         ResultSet rs = null;

         ArrayList<String> tables = new ArrayList<String>();
         try {
             dbmeta = conn.getMetaData();
             rs = dbmeta.getTables(null,"CORNELLDB","%",null);

             ResultSetMetaData rsmd = rs.getMetaData();
             int mdcolumnCount = rsmd.getColumnCount();
             System.out.println("Num columns from getTables: "+mdcolumnCount);
             while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
             }

          } catch (SQLException e) {
            System.out.println("getPrimaryKeys: Problem accessing DatabaseMetaData");
            return null;
          }
          return tables;
     }

     public static void describeTable(Connection conn, String table) {
        ResultSet rs = null;
        HashMap<String, String> columns = new HashMap<String, String>();
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           dbmeta = conn.getMetaData();
           rs = dbmeta.getColumns(null,"CORNELLDB",table,null);
           if (rs == null ) {
              System.out.println("Resultset is null from dbmeta.getColumns() call");
           }

           while (rs.next()) {
              String columnName = rs.getString("COLUMN_NAME");
              String typeName = rs.getString("TYPE_NAME");
              String columnSize = rs.getString("COLUMN_SIZE");
              String digits = rs.getString("DECIMAL_DIGITS");
              String columnType = new String();
              if (typeName.equals("VARCHAR2") || typeName.equals("CHAR") || typeName.equals("DATE")) {
                 columnType = typeName + "("+columnSize+")";
              } else if (typeName.equals("NUMBER")) {
                  columnType = typeName + "("+columnSize+")";
              } else {
                  columnType = typeName;
              }

              columns.put(columnName, columnType);
           }

        } catch (SQLException ex) {
           System.out.println(ex.getMessage());

        } finally {
           try {
              if (rs != null) rs.close();
           }catch (Exception ex) {}
        }
        System.out.println("Table: "+table);
        Iterator iter = columns.keySet().iterator();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            String value = columns.get(key);
            System.out.println("\t"+key+" "+value );
        }
        System.out.println();
     }

     public static void getPrimaryKeys(Connection conn, String table) {
        ResultSet rs = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           dbmeta = conn.getMetaData();
           rs = dbmeta.getPrimaryKeys(null,"CORNELLDB",table);
           while (rs.next()) {
                System.out.println(rs.getString("PK_NAME"));
           }
        } catch (SQLException ex) {
           System.out.println(ex.getMessage());
        } finally {
           try {
              if (rs != null) rs.close();
           } catch (Exception ex) {}
        }
     }

     public static void getEquipment(Connection conn, String bibid) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           stmt = conn.createStatement();
           String sql = "SELECT * FROM CORNELLDB.CIRC_TRANSACTIONS WHERE CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID = " +bibid;
           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           System.out.println("Equipment");
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


     public static void getProjectorRoom(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           stmt = conn.createStatement();

           String sql = ""+
          "SELECT CORNELLDB.ITEM.ITEM_ID, CORNELLDB.BIB_TEXT.BIB_ID, CORNELLDB.MFHD_ITEM.ITEM_ENUM, CORNELLDB.BIB_TEXT.TITLE, CORNELLDB.CIRC_TRANSACTIONS.CURRENT_DUE_DATE, CORNELLDB.MFHD_ITEM.CHRON " +
          "FROM CORNELLDB.ITEM, CORNELLDB.BIB_ITEM, CORNELLDB.BIB_TEXT, CORNELLDB.MFHD_ITEM, CORNELLDB.CIRC_TRANSACTIONS " +
          "WHERE CORNELLDB.ITEM.ITEM_ID = CORNELLDB.BIB_ITEM.ITEM_ID "+
          "AND CORNELLDB.BIB_ITEM.BIB_ID = CORNELLDB.BIB_TEXT.BIB_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.MFHD_ITEM.ITEM_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID (+) "+
          "AND CORNELLDB.BIB_TEXT.BIB_ID = 4005104 "+
          "AND CORNELLDB.MFHD_ITEM.ITEM_ENUM LIKE '%PROJECTOR%' "+
          "ORDER BY CORNELLDB.MFHD_ITEM.CHRON";

           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           System.out.println("Projector Rooms");
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

     public static void getGroupStudyRoom(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           stmt = conn.createStatement();

           String sql = ""+
          "SELECT CORNELLDB.ITEM.ITEM_ID, CORNELLDB.BIB_TEXT.BIB_ID, CORNELLDB.MFHD_ITEM.ITEM_ENUM, CORNELLDB.BIB_TEXT.TITLE, CORNELLDB.CIRC_TRANSACTIONS.CURRENT_DUE_DATE, CORNELLDB.MFHD_ITEM.CHRON " +
          "FROM CORNELLDB.ITEM, CORNELLDB.BIB_ITEM, CORNELLDB.BIB_TEXT, CORNELLDB.MFHD_ITEM, CORNELLDB.CIRC_TRANSACTIONS " +
          "WHERE CORNELLDB.ITEM.ITEM_ID = CORNELLDB.BIB_ITEM.ITEM_ID "+
          "AND CORNELLDB.BIB_ITEM.BIB_ID = CORNELLDB.BIB_TEXT.BIB_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.MFHD_ITEM.ITEM_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID (+) "+
          "AND CORNELLDB.BIB_TEXT.BIB_ID = 4005104 "+
          "AND CORNELLDB.MFHD_ITEM.ITEM_ENUM LIKE 'GROUP%'" +
          "AND CORNELLDB.MFHD_ITEM.ITEM_ENUM NOT LIKE '%PROJECTOR%'" +
          "AND CORNELLDB.MFHD_ITEM.CHRON NOT LIKE '%C.2'"+
          "ORDER BY CORNELLDB.MFHD_ITEM.CHRON";

           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           System.out.println("Group Study Rooms");
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
           }           catch (Exception ex) {}
        }
        System.out.println();
     }

     public static void getGradStudyRoom(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           stmt = conn.createStatement();

           String sql = ""+
          "SELECT CORNELLDB.ITEM.ITEM_ID, CORNELLDB.BIB_TEXT.BIB_ID, CORNELLDB.MFHD_ITEM.ITEM_ENUM, CORNELLDB.BIB_TEXT.TITLE, CORNELLDB.CIRC_TRANSACTIONS.CURRENT_DUE_DATE, CORNELLDB.MFHD_ITEM.CHRON " +
          "FROM CORNELLDB.ITEM, CORNELLDB.BIB_ITEM, CORNELLDB.BIB_TEXT, CORNELLDB.MFHD_ITEM, CORNELLDB.CIRC_TRANSACTIONS " +
          "WHERE CORNELLDB.ITEM.ITEM_ID = CORNELLDB.BIB_ITEM.ITEM_ID "+
          "AND CORNELLDB.BIB_ITEM.BIB_ID = CORNELLDB.BIB_TEXT.BIB_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.MFHD_ITEM.ITEM_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID (+) "+
          "AND CORNELLDB.BIB_TEXT.BIB_ID = 4148896 "+
          "ORDER BY CORNELLDB.MFHD_ITEM.ITEM_ENUM";

           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           System.out.println("Grad Study Rooms");
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

     public static void getIndividualStudyRoom(Connection conn) {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        HashMap<String, String> keys = new HashMap<String, String>();
        try {
           stmt = conn.createStatement();

           String sql = ""+
          "SELECT CORNELLDB.ITEM.ITEM_ID, CORNELLDB.BIB_TEXT.BIB_ID, CORNELLDB.MFHD_ITEM.ITEM_ENUM, CORNELLDB.BIB_TEXT.TITLE, CORNELLDB.CIRC_TRANSACTIONS.CURRENT_DUE_DATE, CORNELLDB.MFHD_ITEM.CHRON " +
          "FROM CORNELLDB.ITEM, CORNELLDB.BIB_ITEM, CORNELLDB.BIB_TEXT, CORNELLDB.MFHD_ITEM, CORNELLDB.CIRC_TRANSACTIONS " +
          "WHERE CORNELLDB.ITEM.ITEM_ID = CORNELLDB.BIB_ITEM.ITEM_ID "+
          "AND CORNELLDB.BIB_ITEM.BIB_ID = CORNELLDB.BIB_TEXT.BIB_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.MFHD_ITEM.ITEM_ID "+
          "AND CORNELLDB.ITEM.ITEM_ID = CORNELLDB.CIRC_TRANSACTIONS.ITEM_ID (+) "+
          "AND CORNELLDB.BIB_TEXT.BIB_ID = 4005104 "+
          "AND CORNELLDB.MFHD_ITEM.ITEM_ENUM LIKE 'INDIVIDUAL%'" +
          "AND CORNELLDB.MFHD_ITEM.CHRON NOT LIKE '%C.2'"+
          "ORDER BY CORNELLDB.MFHD_ITEM.CHRON";

           rs = stmt.executeQuery(sql);
           rsmd = rs.getMetaData();
           int mdcolumnCount = rsmd.getColumnCount();
           System.out.println("Individual Study Rooms");
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

}
