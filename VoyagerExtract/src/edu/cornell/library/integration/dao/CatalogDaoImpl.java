package edu.cornell.library.integration.dao;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Clob;

import oracle.jdbc.OracleResultSet;
import oracle.sql.CLOB;
import java.text.SimpleDateFormat; 
import java.util.Calendar; 
import java.util.List; 

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
 
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.dao.CatalogDao; 

public class CatalogDaoImpl implements CatalogDao {
   /**
    * logger
    */
   protected final Log logger = LogFactory.getLog(getClass());
   protected DataSource dataSource;
   private SimpleJdbcTemplate simpleJdbcTemplate;
   private JdbcTemplate jdbcTemplate;
   // holders for fields passed as objects that need to be picked up by the RowMappers
   protected int currentLocationId;
   protected String currentHoldingsId;

   /**
    *
    */
   public CatalogDaoImpl() {
      super();
   }

   /**
    * @return
    */
   public DataSource getDataSource() {
      return dataSource;
   }

   /**
    * @param dataSource
    */
   public void setDataSource(DataSource dataSource) {
      this.simpleJdbcTemplate = new SimpleJdbcTemplate(dataSource);
      this.jdbcTemplate = new JdbcTemplate(dataSource);
   }

   /**
    * @return the currentLocationId
    */
   public int getCurrentLocationId() {
      return currentLocationId;
   }

   /**
    * @param currentLocationId the currentLocationId to set
    */
   public void setCurrentLocationId(int currentLocationId) {
      this.currentLocationId = currentLocationId;
   }

   /**
    * @return the currentHoldingsId
    */
   public String getCurrentHoldingsId() {
      return currentHoldingsId;
   }

   /**
    * @param currentHoldingsId the currentHoldingsId to set
    */
   public void setCurrentHoldingsId(String currentHoldingsId) {
      this.currentHoldingsId = currentHoldingsId;
   }

   /*
    * (non-Javadoc)
    *
    * @see edu.cornell.library.integration.dao.CatalogDao#sanityCheck()
    */
   public int sanityCheck() {
      String sql = "SELECT 1";
      return jdbcTemplate.queryForInt(sql);
   }
   
   public List<Location> getAllLocation() throws Exception{
      String sql = new String();
      //sql = "SELECT * FROM CORNELLDB.LOCATION";
      sql = ""
      +"SELECT "
      +" LOCATION.LOCATION_CODE, "
      +" LOCATION.LOCATION_ID, "
      +" LOCATION.SUPPRESS_IN_OPAC, "
      +" LOCATION.MFHD_COUNT, "
      +" LOCATION.LOCATION_OPAC, "
      +" LOCATION.LOCATION_NAME, "
      +" LOCATION.LOCATION_SPINE_LABEL, "
      +" LOCATION.LOCATION_DISPLAY_NAME, "
      +" LOCATION.LIBRARY_ID "
      +" FROM LOCATION ";
       
      //System.out.println(sql);
      try {
          List<Location> locationList =  this.jdbcTemplate.query(sql, new LocationMapper());
          return locationList;
       } catch (EmptyResultDataAccessException ex) {
          logger.warn("Empty result set");
          logger.info("Query was: "+sql);
          return null;
       } catch (Exception ex) {
          logger.error("Exception: ", ex);
          logger.info("Query was: "+sql);
          throw ex;
       }

      
   }
   
   public List<String> getRecentBibIds() throws Exception {
       
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Calendar now = Calendar.getInstance();
      Calendar earlier = now;
      earlier.add(Calendar.HOUR, -3);
      String ds = df.format(earlier.getTime());
      
      String sql = "" 
            +" SELECT BIB_ID FROM BIB_HISTORY"
            +" WHERE to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + ds + "'"   
            +" AND SUPPRESS_IN_OPAC = 'N'";
      try {
         List<String> bibIdList =  this.jdbcTemplate.query(sql, new StringMapper());
         return bibIdList;
      } catch (EmptyResultDataAccessException ex) {
         logger.warn("Empty result set");
         logger.info("Query was: "+sql);
         return null;
      } catch (Exception ex) {
         logger.error("Exception: ", ex);
         logger.info("Query was: "+sql);
         throw ex;
      } 
   }
   
   public BibData getBibData(String bibid) throws Exception {
       
      String sql = ""
         +"SELECT BIB_ID, MARC_RECORD FROM CORNELLDB.BIBBLOB_VW WHERE BIB_ID = '"+ bibid +"'";
      try {
         BibData bibData =  (BibData) this.jdbcTemplate.queryForObject(sql, new BibDataMapper());
         return bibData;
      } catch (EmptyResultDataAccessException ex) {
         logger.warn("Empty result set");
         logger.info("Query was: "+sql);
         return null;
      } catch (Exception ex) {
         logger.error("Exception: ", ex);
         logger.info("Query was: "+sql);
         throw ex;
      }  
      
   }




   protected void displayMetaData(ResultSet rs) {
      ResultSetMetaData rsmeta = null;
      try {
         rsmeta = rs.getMetaData();
         int numcol = rsmeta.getColumnCount();
         logger.info("found numcol: "+numcol);
         for (int i=1 ; i < numcol ; i++) {
            logger.info("db column: "+ rsmeta.getColumnName(i));
         }
      } catch (SQLException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }
   
   /**
    * @param clob
    * @return
    * @throws Exception
    */
   protected static String convertClobToString(Clob clob)  { 
        
      try {        
         System.out.println("clob length: "+ clob.length());
         char clobVal[] = new char[(int) clob.length()];         
         Reader reader = clob.getCharacterStream();
         reader.read(clobVal);
         StringWriter sw = new StringWriter();
         sw.write(clobVal);
         StringBuffer sb = sw.getBuffer();
         return sb.toString();            
         
      } catch(Exception e) { 
         e.printStackTrace();
         return "";
      }   
      
   }
   
   
   
   private static final class LocationMapper implements RowMapper {
      public Location mapRow(ResultSet rs, int rowNum) throws SQLException {
         Location location = new Location(); 
         location.setLocationId(rs.getString("LOCATION_ID"));
         location.setLocationCode(rs.getString("LOCATION_CODE"));
         location.setSuppressInOpac(rs.getString("SUPPRESS_IN_OPAC"));
         location.setMfhdCount(rs.getInt("MFHD_COUNT"));
         location.setLibraryId(rs.getString("LIBRARY_ID"));
         location.setLocationOpac(rs.getString("LOCATION_OPAC"));
         location.setLocationSpineLabel(rs.getString("LOCATION_SPINE_LABEL"));
         location.setLocationDisplayName(rs.getString("LOCATION_DISPLAY_NAME"));   
         location.setLocationName(rs.getString("LOCATION_NAME"));   
         //location.setLibraryName(rs.getString("LIBRARY_NAME"));
        // location.setLibraryDisplayName(rs.getString("LIBRARY_DISPLAY_NAME"));
         return location;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class BibDataMapper implements RowMapper {
      public BibData mapRow(ResultSet rs, int rowNum) throws  SQLException {
         BibData bibData = new BibData(); 
         bibData.setBibId(rs.getString("BIB_ID")); 
         Clob clob =   rs.getClob("MARC_RECORD");
         bibData.setClob(clob);
         bibData.setRecord(convertClobToString(clob));
         return bibData;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class StringMapper implements RowMapper {
      public String mapRow(ResultSet rs, int rowNum) throws SQLException {
         String s = new String();
         s = rs.getString(1);
         return s;
      }
   }

}
