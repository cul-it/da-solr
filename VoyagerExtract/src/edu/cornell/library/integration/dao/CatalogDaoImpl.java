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
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
 
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

}
