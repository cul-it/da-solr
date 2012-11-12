package edu.cornell.library.integration.dao;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
 
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

}
