package edu.cornell.library.integration.dao;


 
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import oracle.sql.CLOB;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcDaoSupport;
import org.springframework.jdbc.support.lob.OracleLobHandler;

import edu.cornell.library.integration.bo.AuthData;
import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.bo.MfhdMasterData;

public class CatalogDaoImpl extends SimpleJdbcDaoSupport implements CatalogDao {
   /**
    * logger
    */
   protected final Log logger = LogFactory.getLog(getClass());
   protected DataSource dataSource;
   private OracleLobHandler oracleLobHandler; 
   
   public CatalogDaoImpl() {
      super();
   }

   public OracleLobHandler getOracleLobHandler() {
           return oracleLobHandler;
   }

   public void setOracleLobHandler(OracleLobHandler oracleLobHandler) {
           this.oracleLobHandler = oracleLobHandler;
   }


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
            +" WHERE SUPPRESS_IN_OPAC = 'N'"
            +" AND to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'";   
            
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
   
   public List<String> getUpdatedBibIdsUsingDateRange(String fromDate, String toDate) throws Exception {
       
	      String sql = "" 
	            +" SELECT BIB_ID FROM BIB_HISTORY"
	            +" WHERE SUPPRESS_IN_OPAC = 'N'"
	            +" AND to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + fromDate +"'"
	            +" AND to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') < '" + toDate +"'";   
	            
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
            +" WHERE SUPPRESS_IN_OPEN = 'N"
            +" AND to_char(BIB_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'";   
             
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
   
   public BibMasterData getBibMasterData(String bibid) throws Exception {
      
      String sql = ""
         +"SELECT BIB_ID, SUPPRESS_IN_OPAC, CREATE_DATE, UPDATE_DATE FROM CORNELLDB.BIB_MASTER WHERE BIB_ID = '"+ bibid +"'";
      try {
         BibMasterData bibMasterData =  (BibMasterData) getSimpleJdbcTemplate().queryForObject(sql, new BibMasterMapper());
         return bibMasterData;
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
   
public MfhdMasterData getMfhdMasterData(String mfhdid) throws Exception {
      
      String sql = ""
         +"SELECT BIB_ID, SUPPRESS_IN_OPAC, CREATE_DATE, UPDATE_DATE FROM CORNELLDB.MFHD_MASTER WHERE BIB_ID = '"+ mfhdid +"'";
      try {
         MfhdMasterData mfhdMasterData =  (MfhdMasterData) getSimpleJdbcTemplate().queryForObject(sql, new MfhdMasterMapper());
         return mfhdMasterData;
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
         +"SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = '"+ bibid +"'  ORDER BY BIB_DATA.SEQNUM";
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
   
   public List<AuthData> getAuthData(String authid) throws Exception {
      
      String sql = ""
         +"SELECT * FROM AUTH_DATA WHERE AUTH_DATA.AUTH_ID = "+ authid +"  ORDER BY AUTH_DATA.SEQNUM";
      try {
         List<AuthData> authDataList =  getSimpleJdbcTemplate().query(sql, new AuthDataMapper());
         return authDataList;
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
   public List<String> getRecentAuthIds(String dateString) throws Exception {
       
      String sql = "" 
            +" SELECT AUTH_ID FROM AUTH_HISTORY"
            +" WHERE to_char(AUTH_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'";   
            
      try {
         List<String> authIdList =  getSimpleJdbcTemplate().query(sql, new StringMapper());
         return authIdList;
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
      String sql = "" 
            +" SELECT MFHD_ID FROM MFHD_HISTORY"
            +" WHERE SUPPRESS_IN_OPAC = 'N'"
            +" AND to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + dateString + "'";

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
   
   public List<String> getUpdatedMfhdIdsUsingDateRange(String fromDate, String toDate) throws Exception { 
	      String sql = "" 
	            +" SELECT MFHD_ID FROM MFHD_HISTORY"
	            +" WHERE SUPPRESS_IN_OPAC = 'N'"
	            +" AND to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') > '" + fromDate +"'"
	            +" AND to_char(MFHD_HISTORY.ACTION_DATE, 'yyyy-MM-dd HH:mm:ss') < '" + toDate +"'";

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
   
   public List<Integer> getAllSuppressedBibId() throws Exception {
      String sql = "" 
            +" SELECT BIB_ID FROM BIB_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'Y'";

      try {
         return  getSimpleJdbcTemplate().query(sql, new IntegerMapper());         
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
   
   public List<Integer> getAllUnSuppressedBibId() throws Exception {
      String sql = "" 
            +" SELECT BIB_ID FROM BIB_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'N'";

      try {
         List<Integer> bibIdList =  getSimpleJdbcTemplate().query(sql, new IntegerMapper());
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
   
   public List<String> getSuppressedBibId(String fromDateString,
         String toDateString) throws Exception {
      String sql = "" 
            +" SELECT BIB_ID FROM BIB_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'Y'"
            +" AND to_char(BIB_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') BETWEEN '" + fromDateString + "'"   
            +" AND '" + toDateString + "'";
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
   
   public List<Integer> getAllSuppressedMfhdId() throws Exception {
      String sql = "" 
            +" SELECT MFHD_ID FROM MFHD_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'Y'";

      try {
         return  getSimpleJdbcTemplate().query(sql, new IntegerMapper());         
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
   
   public List<Integer> getAllUnSuppressedMfhdId() throws Exception {
      String sql = "" 
            +" SELECT MFHD_ID FROM MFHD_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'N'";

      try {
         List<Integer> mfhdIdList =  getSimpleJdbcTemplate().query(sql, new IntegerMapper());
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

	public int saveAllUnSuppressedBibsWithDates(Path outputFile) throws Exception {
	      String sql = ""
	              +" SELECT BIB_ID, to_char(UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') FROM BIB_MASTER"
	              +" WHERE SUPPRESS_IN_OPAC = 'N'";
	      final int bibCount[] ={ 0 };
	       try( BufferedWriter out = Files.newBufferedWriter(outputFile, Charset.forName("UTF-8")); ){
	    	   getJdbcTemplate().query(sql,new RowCallbackHandler(){
	    		    @Override public void processRow( ResultSet rs) throws SQLException {
	    		      String id=rs.getString(1);
	    		      String date=rs.getString(2);
	    		      bibCount[0]++;
	    		      try {
	    		    	  out.write(id+"|"+date+"\n");
	    		      } catch (IOException e) {
	    		    	  e.printStackTrace();
	    		      }
	    		    }
	    		  }
	    		);
	       }
	       return bibCount[0];
	}

	public int saveAllUnSuppressedMfhdsWithDates(Path outputFile) throws Exception {
	      String sql = ""
	              +" select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, to_char(UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss')"
	    		  +"   from BIB_MFHD, MFHD_MASTER"
	              +"  where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID and SUPPRESS_IN_OPAC = 'N'";
	      final int mfhdCount[] ={ 0 };
	       try( BufferedWriter out = Files.newBufferedWriter(outputFile, Charset.forName("UTF-8")); ){
	    	   getJdbcTemplate().query(sql,new RowCallbackHandler(){
	    		    @Override public void processRow( ResultSet rs) throws SQLException {
	    		      String bib=rs.getString(1);
	    		      String mfhd=rs.getString(2);
	    		      String date=rs.getString(3);
	    		      mfhdCount[0]++;
	    		      try {
	    		    	  out.write(bib+"|"+mfhd+"|"+date+"\n");
	    		      } catch (IOException e) {
	    		    	  e.printStackTrace();
	    		      }
	    		    }
	    		  }
	    		);
	       }
	       return mfhdCount[0];
	}

	public int saveAllItemMaps(Path outputFile) throws Exception {
		String sql = ""
				+"SELECT BIB_MFHD.BIB_ID, BIB_MFHD.MFHD_ID, ITEM.ITEM_ID,"
				+ "         to_char(ITEM.MODIFY_DATE, 'yyyy-MM-dd HH:mm:ss') as MODIFY_DATE"
				+"  FROM BIB_MFHD, MFHD_ITEM, ITEM"
				+" WHERE BIB_MFHD.MFHD_ID = MFHD_ITEM.MFHD_ID"
				+"   AND MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID";
		final int itemCount[] ={ 0 };
	       try( BufferedWriter out = Files.newBufferedWriter(outputFile, Charset.forName("UTF-8")); ){
	    	   getJdbcTemplate().query(sql,new RowCallbackHandler(){
	    		    @Override public void processRow( ResultSet rs) throws SQLException {
	    		      String bib=rs.getString(1);
	    		      String mfhd=rs.getString(2);
	    		      String item=rs.getString(3);
	    		      String date=rs.getString(4);
	    		      itemCount[0]++;
	    		      try {
	    		    	  out.write(bib+"|"+mfhd+"|"+item+"|"+date+"\n");
	    		      } catch (IOException e) {
	    		    	  e.printStackTrace();
	    		      }
	    		    }
	    		  }
	    		);
	       }
	       return itemCount[0];
	}

   public List<String> getSuppressedMfhdId(String fromDateString,
         String toDateString) throws Exception {
      String sql = "" 
            +" SELECT to_char(MFHD_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss'), MFHD_ID FROM MFHD_MASTER"
            +" WHERE SUPPRESS_IN_OPAC = 'Y'"
            +" AND to_char(MFHD_MASTER.UPDATE_DATE, 'yyyy-MM-dd HH:mm:ss') BETWEEN '" + fromDateString + "'"   
            +" AND '" + toDateString + "'";

      try {
         List<String> mfhdIdList =  getSimpleJdbcTemplate().query(sql, new TabbedStringMapper());
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
   
	public List<String> getBibIdsByMfhdId(String mfhdid) throws Exception {
		String sql = "SELECT BIB_ID from BIB_MFHD WHERE MFHD_ID='" + mfhdid + "'";
		try {
			List<String> bibIdList = getSimpleJdbcTemplate().query(sql,
					new StringMapper ());
			return bibIdList;
		} catch (EmptyResultDataAccessException ex) {
			logger.warn("Empty result set");
			logger.info("Query was: " + sql);
			return null;
		} catch (Exception ex) {
			logger.error("Exception: ", ex);
			logger.info("Query was: " + sql);
			throw ex;
		}
	}
	
	public List<String> getMfhdIdsByBibId(String bibid) throws Exception {
		String sql = "SELECT MFHD_ID from BIB_MFHD WHERE BIB_ID='" + bibid + "'";
		try {
			List<String> mfhdIdList = getSimpleJdbcTemplate().query(sql, new StringMapper ());
			return mfhdIdList;
		} catch (EmptyResultDataAccessException ex) {
			logger.warn("Empty result set");
			logger.info("Query was: " + sql);
			return null;
		} catch (Exception ex) {
			logger.error("Exception: ", ex);
			logger.info("Query was: " + sql);
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

   private static final class LocationMapper implements RowMapper<Location> {
      public Location mapRow(ResultSet rs, int rowNum) throws SQLException {
         Location location = new Location(); 
         location.setLocationId(rs.getString("LOCATION_ID"));
         location.setLocationCode(rs.getString("LOCATION_CODE"));
         location.setSuppressInOpac(rs.getString("SUPPRESS_IN_OPAC"));
         location.setMfhdCount(rs.getInt("MFHD_COUNT"));
         location.setLibraryId(rs.getString("LIBRARY_ID"));
         if (StringUtils.isEmpty(rs.getString("LOCATION_OPAC"))) {
            location.setLocationOpac("N");
         } else {
            location.setLocationOpac(rs.getString("LOCATION_OPAC"));
         }
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
   private static final class BibDataMapper implements RowMapper<BibData> {
      public BibData mapRow(ResultSet rs, int rowNum) throws  SQLException  {
         BibData bibData = new BibData(); 
         bibData.setBibId(rs.getString("BIB_ID"));
         bibData.setSeqnum(rs.getString("SEQNUM"));
         
         String record = "";
         try {
            record = new String(rs.getBytes("RECORD_SEGMENT"), "UTF-8");
         } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
         } 
          
         bibData.setRecord(record); 
         return bibData;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class MfhdDataMapper implements RowMapper<MfhdData> {
      public MfhdData mapRow(ResultSet rs, int rowNum) throws  SQLException {
         MfhdData mfhdData = new MfhdData(); 
         mfhdData.setMfhdId(rs.getString("MFHD_ID"));
         mfhdData.setSeqnum(rs.getString("SEQNUM"));
         
         String record = "";
         try {
            record = new String(rs.getBytes("RECORD_SEGMENT"), "UTF-8");             
         } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
         }
          
         mfhdData.setRecord(record);
         return mfhdData;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class AuthDataMapper implements RowMapper<AuthData> {
      public AuthData mapRow(ResultSet rs, int rowNum) throws  SQLException  {
         AuthData authData = new AuthData(); 
         authData.setAuthId(rs.getString("AUTH_ID"));
         authData.setSeqnum(rs.getString("SEQNUM"));
         
         String record = "";
         try {
            record = new String(rs.getBytes("RECORD_SEGMENT"), "UTF-8");
         } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
         } 
          
         authData.setRecord(record); 
         return authData;
       }
   }
   
   private static final class BibBlobMapper implements RowMapper<BibBlob> {
      public BibBlob mapRow(ResultSet rs, int rowNum) throws  SQLException {
         BibBlob bibBlob = new BibBlob(); 
         bibBlob.setBibId(rs.getString("BIB_ID"));
         bibBlob.setClob((CLOB) rs.getClob("MARC_RECORD"));
         return bibBlob;
       }
   }
   
   private static final class MfhdBlobMapper implements RowMapper<MfhdBlob> {
      public MfhdBlob mapRow(ResultSet rs, int rowNum) throws  SQLException {
         MfhdBlob mfhdBlob = new MfhdBlob(); 
         mfhdBlob.setMfhdId(rs.getString("MFHD_ID"));
         mfhdBlob.setClob((CLOB) rs.getClob("MARC_RECORD"));
         return mfhdBlob;
       }
   }
   
   private static final class BibMasterMapper implements RowMapper<BibMasterData> {
      public BibMasterData mapRow(ResultSet rs, int rowNum) throws  SQLException {
         BibMasterData bibMasterData = new BibMasterData(); 
         bibMasterData.setBibId(rs.getString("BIB_ID"));
         bibMasterData.setSuppressed(rs.getString("SUPPRESS_IN_OPAC"));
         bibMasterData.setCreateDate(rs.getString("CREATE_DATE"));
         bibMasterData.setUpdateDate(rs.getString("UPDATE_DATE"));
         return bibMasterData;
       }
   }
   
   private static final class MfhdMasterMapper implements RowMapper<MfhdMasterData> {
      public MfhdMasterData mapRow(ResultSet rs, int rowNum) throws  SQLException {
         MfhdMasterData mfhdMasterData = new MfhdMasterData(); 
         mfhdMasterData.setMfhdId(rs.getString("MFHD_ID"));
         mfhdMasterData.setSuppressed(rs.getString("SUPPRESS_IN_OPAC"));
         mfhdMasterData.setCreateDate(rs.getString("CREATE_DATE"));
         mfhdMasterData.setUpdateDate(rs.getString("UPDATE_DATE"));
         return mfhdMasterData;
       }
   }
   
   /**
    * @author jaf30
    *
    */
   private static final class StringMapper implements RowMapper<String> {
      public String mapRow(ResultSet rs, int rowNum) throws SQLException {
         String s = new String();
         s = rs.getString(1);
         return s;
      }
   }
   
   private static final class IntegerMapper implements RowMapper<Integer>{
       public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {           
           return Integer.parseInt( rs.getString(1) );           
        }
   }
   
   private static final class TabbedStringMapper implements RowMapper<String> {
      public String mapRow(ResultSet rs, int rowNum) throws SQLException {
         ResultSetMetaData rsmd = rs.getMetaData();
         StringBuilder sb = new StringBuilder();
         int colnum = rsmd.getColumnCount();
         for (int i=1; i <= colnum; i++) {
            sb.append(rs.getString(i));
            if (i < colnum) {
               sb.append("\t");
            }
         }
         return sb.toString();
      }
   }
}
