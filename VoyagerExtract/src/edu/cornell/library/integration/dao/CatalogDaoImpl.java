package edu.cornell.library.integration.dao;


import java.io.IOException;
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
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.support.lob.OracleLobHandler;
 
import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.dao.CatalogDao; 

public class CatalogDaoImpl extends SimpleJdbcDaoSupport implements CatalogDao {
   /**
    * logger
    */
   protected final Log logger = LogFactory.getLog(getClass());
   protected DataSource dataSource;
   private SimpleJdbcTemplate simpleJdbcTemplate;
   private JdbcTemplate jdbcTemplate;
   private OracleLobHandler oracleLobHandler; 
   /**
    *
    */
   public CatalogDaoImpl() {
      super();
   }
   
   

   /**
    * @return the oracleLobHandler
    */
   public OracleLobHandler getOracleLobHandler() {
           return oracleLobHandler;
   }

   public void setOracleLobHandler(OracleLobHandler oracleLobHandler) {
           this.oracleLobHandler = oracleLobHandler;
   }


   /**
    * @return
    */
   /*public DataSource getDataSource() {
      return dataSource;
   }*/

   /**
    * @param dataSource
    */
  /* public void setDataSource(DataSource dataSource) {
      this.simpleJdbcTemplate = new SimpleJdbcTemplate(dataSource);
      this.jdbcTemplate = new JdbcTemplate(dataSource);
   }*/

   
 

   /*
    * (non-Javadoc)
    *
    * @see edu.cornell.library.integration.dao.CatalogDao#sanityCheck()
    */
   public int sanityCheck() {
      String sql = "SELECT 1";
      return getSimpleJdbcTemplate().queryForInt(sql);
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getAllLocation()
    */
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
          List<Location> locationList =  getSimpleJdbcTemplate().query(sql, new LocationMapper());
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
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getRecentBibIds(java.lang.String)
    */
   public List<String> getRecentBibIds(String dateString) throws Exception {
       
      String sql = "" 
            +" SELECT BIB_ID FROM BIB_HISTORY"
            +" WHERE to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'"   
            +" AND SUPPRESS_IN_OPAC = 'N'";
      try {
         List<String> bibIdList =  getSimpleJdbcTemplate().query(sql, new StringMapper());
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
   
   
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getRecentBibIdCount(java.lang.String)
    */
   public int getRecentBibIdCount(String dateString) throws Exception {      
           
      String sql = "" 
            +" SELECT COUNT(BIB_ID) FROM BIB_HISTORY"
            +" WHERE to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'"   
            +" AND SUPPRESS_IN_OPAC = 'N'";
      try {
         int count =  getSimpleJdbcTemplate().queryForInt(sql);
         return count;
      } catch (EmptyResultDataAccessException ex) {
         logger.warn("Empty result set");
         logger.info("Query was: "+sql);
         return 0;
      } catch (Exception ex) {
         logger.error("Exception: ", ex);
         logger.info("Query was: "+sql);
         throw ex;
      } 
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getBibBlob(java.lang.String)
    */
   public BibBlob getBibBlob(String bibid) throws Exception {
       
      String sql = ""
         +"SELECT BIB_ID, MARC_RECORD FROM CORNELLDB.BIBBLOB_VW WHERE BIB_ID = '"+ bibid +"'";
      try {
         BibBlob bibBlob =  (BibBlob) getSimpleJdbcTemplate().queryForObject(sql, new BibBlobMapper());
         return bibBlob;
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
   
   public MfhdBlob getMfhdBlob(String mfhdid) throws Exception {
       
      String sql = ""
         +"SELECT MFHD_ID, MARC_RECORD FROM CORNELLDB.MFHDBLOB_VW WHERE MFHD_ID = '"+ mfhdid +"'";
      try {
         MfhdBlob mfhdBlob =  (MfhdBlob) getSimpleJdbcTemplate().queryForObject(sql, new MfhdBlobMapper());
         return mfhdBlob;
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
   
   public List<BibData> getBibData(String bibid) throws Exception {
      
      String sql = ""
         +"SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = "+ bibid +"  ORDER BY BIB_DATA.SEQNUM";
      try {
         List<BibData> bibDataList =  getSimpleJdbcTemplate().query(sql, new BibDataMapper());
         return bibDataList;
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
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getRecentMfidIds(java.lang.String)
    */
   public List<String> getRecentMfhdIds(String dateString) throws Exception {
       
      //String sql = "" 
      //      +" SELECT MFHD_ID FROM MFHDHISTORY_VW"
      //      +" WHERE to_char(MFHDHISTORY_VW.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'"   
      //      +" OR to_char(MFHDHISTORY_VW.CREATE_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'";   

      String sql = "" 
            +" SELECT MFHD_ID FROM MFHD_HISTORY"
            +" WHERE to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'"   
            +" AND SUPPRESS_IN_OPAC = 'N'";

      try {
         List<String> mfhdIdList =  getSimpleJdbcTemplate().query(sql, new StringMapper());
         return mfhdIdList;
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
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getRecentMfhdIdCount(java.lang.String)
    */
   public int getRecentMfhdIdCount(String dateString) throws Exception {      
           
      String sql = "" 
            +" SELECT COUNT(MFHD_ID) FROM MFHD_HISTORY"
            +" WHERE to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'"   
            +" AND SUPPRESS_IN_OPAC = 'N'";
      try {
         int count =  getSimpleJdbcTemplate().queryForInt(sql);
         return count;
      } catch (EmptyResultDataAccessException ex) {
         logger.warn("Empty result set");
         logger.info("Query was: "+sql);
         return 0;
      } catch (Exception ex) {
         logger.error("Exception: ", ex);
         logger.info("Query was: "+sql);
         throw ex;
      } 
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.dao.CatalogDao#getMfhdData(java.lang.String)
    */
   public List<MfhdData> getMfhdData(String mfhdid) throws Exception {
      
      String sql = ""
         +"SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = "+ mfhdid +"  ORDER BY MFHD_DATA.SEQNUM";
      try {
         List<MfhdData> mfhdDataList =  getSimpleJdbcTemplate().query(sql, new MfhdDataMapper());
         return mfhdDataList;
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




   /**
    * @param rs
    */
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
          
         char[] clobVal = new char[(int) clob.length()];         
         Reader reader = clob.getCharacterStream();
         reader.read(clobVal);
         String str = new String(clobVal);
         return str;            
         
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
         bibData.setSeqnum(rs.getString("SEQNUM"));
         bibData.setRecord(rs.getString("RECORD_SEGMENT"));
         return bibData;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class MfhdDataMapper implements RowMapper {
      public MfhdData mapRow(ResultSet rs, int rowNum) throws  SQLException {
         MfhdData mfhdData = new MfhdData(); 
         mfhdData.setMfhdId(rs.getString("MFHD_ID"));
         mfhdData.setSeqnum(rs.getString("SEQNUM"));
         mfhdData.setRecord(rs.getString("RECORD_SEGMENT"));
         return mfhdData;
       }
   }
   
   private static final class BibBlobMapper implements RowMapper {
      public BibBlob mapRow(ResultSet rs, int rowNum) throws  SQLException {
         BibBlob bibBlob = new BibBlob(); 
         bibBlob.setBibId(rs.getString("BIB_ID"));
         bibBlob.setClob((CLOB) rs.getClob("MARC_RECORD"));
         return bibBlob;
       }
   }
   
   private static final class MfhdBlobMapper implements RowMapper {
      public MfhdBlob mapRow(ResultSet rs, int rowNum) throws  SQLException {
         MfhdBlob mfhdBlob = new MfhdBlob(); 
         mfhdBlob.setMfhdId(rs.getString("MFHD_ID"));
         mfhdBlob.setClob((CLOB) rs.getClob("MARC_RECORD"));
         return mfhdBlob;
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
