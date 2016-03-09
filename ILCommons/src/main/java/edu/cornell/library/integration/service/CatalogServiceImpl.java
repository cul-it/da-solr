package edu.cornell.library.integration.service;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.bo.AuthData;
import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.bo.MfhdMasterData;
import edu.cornell.library.integration.dao.CatalogDao;

public class CatalogServiceImpl implements CatalogService {
    
    private CatalogDao catalogDao;

    /**
     * 
     */
    public CatalogServiceImpl() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @return the catalogDao
     */
    public CatalogDao getCatalogDao() {
        return catalogDao;
    }

    /**
     * @param catalogDao the catalogDao to set
     */
    public void setCatalogDao(CatalogDao catalogDao) {
        this.catalogDao = catalogDao;
    }
    
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAllLocation()
    */
   public List<Location> getAllLocation() throws Exception {
      List<Location> locationList;
      try {
         locationList = catalogDao.getAllLocation();
      } catch (Exception e) {
         throw e;
      }
      return locationList;

   }
   
	public int saveAllUnSuppressedBibsWithDates(Path outputFile) throws Exception {
		return catalogDao.saveAllUnSuppressedBibsWithDates(outputFile);
	}

	public int saveAllUnSuppressedMfhdsWithDates(Path outputFile) throws Exception {
		return catalogDao.saveAllUnSuppressedMfhdsWithDates(outputFile);
	}

	public int saveAllItemMaps(Path outputFile) throws Exception {
		return catalogDao.saveAllItemMaps(outputFile);
	}


   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getRecentBibIds()
    */
   public List<String> getRecentBibIds(String dateString) throws Exception {
      List<String> bibIdList;
      try {
         bibIdList = catalogDao.getRecentBibIds(dateString);
      } catch (Exception e) {
         throw e;
      }
      return bibIdList;
   }
   
	/*
	 * (non-Javadoc)
	 * 
	 * @see edu.cornell.library.integration.service.CatalogService#
	 * getUpdatedBibIdsUsingDateRange(java.lang.String, java.lang.String)
	 */
	public List<String> getUpdatedBibIdsUsingDateRange(String fromDate,
			String toDate) throws Exception {
		List<String> bibIdList;
		try {
			bibIdList = catalogDao.getUpdatedBibIdsUsingDateRange(fromDate,
					toDate);
		} catch (Exception e) {
			throw e;
		}
		return bibIdList;
	}
   
   public int getRecentBibIdCount(String dateString) throws Exception {
      
      try {
         return catalogDao.getRecentBibIdCount(dateString);
      } catch (Exception e) {
         throw e;
      } 
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getRecentMfhdIds()
    */
   public List<String> getRecentMfhdIds(String dateString) throws Exception {
      List<String> mfhdIdList;
      try {
         mfhdIdList = catalogDao.getRecentMfhdIds(dateString);
      } catch (Exception e) {
         throw e;
      }
      return mfhdIdList;
   }
   
	public List<String> getMfhdIdsByBibId(String bibid) throws Exception {
		List<String> mfhdIdList;
		try {
			mfhdIdList = catalogDao.getMfhdIdsByBibId(bibid);
		} catch (Exception e) {
			throw e;
		}
		return mfhdIdList;
	}
	
	public List<String> getBibIdsByMfhdId(String mfhdid) throws Exception {
		List<String> bibIdList;
		try {
			bibIdList = catalogDao.getBibIdsByMfhdId(mfhdid);
		} catch (Exception e) {
			throw e;
		}
		return bibIdList;
	}
	
	
   
   
   
   public List<String> getUpdatedMfhdIdsUsingDateRange(String fromDate, String toDate) throws Exception {
	      List<String> mfhdIdList;
	      try {
	         mfhdIdList = catalogDao.getUpdatedMfhdIdsUsingDateRange(fromDate, toDate);
	      } catch (Exception e) {
	         throw e;
	      }
	      return mfhdIdList;
	   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getRecentMfhdIdCount(java.lang.String)
    */
   public int getRecentMfhdIdCount(String dateString) throws Exception {
      
      try {
         return catalogDao.getRecentMfhdIdCount(dateString);
      } catch (Exception e) {
         throw e;
      } 
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getRecentAuthIds(java.lang.String)
    */
   public List<String> getRecentAuthIds(String dateString) throws Exception {
      
      try {
         return catalogDao.getRecentAuthIds(dateString);
      } catch (Exception e) {
         throw e;
      } 
   }

   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getBibData(java.lang.String)
    */
   public BibBlob getBibBlob(String bibid) throws Exception {
      BibBlob bibBlob = new BibBlob();
      
      try {
         bibBlob = (BibBlob) catalogDao.getBibBlob(bibid);
         return bibBlob;
         
      } catch (Exception e) {
         throw e;
      }
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getMfhdBlob(java.lang.String)
    */
   public MfhdBlob getMfhdBlob(String mfhdid) throws Exception {
      MfhdBlob mfhdBlob = new MfhdBlob();
      
      try {
         mfhdBlob = (MfhdBlob) catalogDao.getMfhdBlob(mfhdid);
         return mfhdBlob;
         
      } catch (Exception e) {
         throw e;
      }
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getBibData(java.lang.String)
    */
   public List<BibData> getBibData(String bibid) throws Exception {
      List<BibData> bibDataList = new ArrayList<BibData>();
      
      try {
         bibDataList = catalogDao.getBibData(bibid);
         return bibDataList;
         
      } catch (Exception e) {
         throw e;
      }
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getMfhdData(java.lang.String)
    */
   public List<MfhdData> getMfhdData(String mfhdid) throws Exception {
      List<MfhdData> mfhdDataList = new ArrayList<MfhdData>();
      
      try {
         mfhdDataList = catalogDao.getMfhdData(mfhdid);
         return mfhdDataList;
         
      } catch (Exception e) {
         throw e;
      }
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAuthData(java.lang.String)
    */
   public List<AuthData> getAuthData(String authid) throws Exception {
      List<AuthData> authDataList = new ArrayList<AuthData>();
      
      try {
         authDataList = catalogDao.getAuthData(authid);
         return authDataList;
         
      } catch (Exception e) {
         throw e;
      }
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAllSuppressedBibId()
    */
   public List<Integer> getAllSuppressedBibId() throws Exception {      
      try {
         return  catalogDao.getAllSuppressedBibId();
      } catch (Exception e) {
         throw e;
      }      
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAllSuppressedBibId()
    */
   public List<Integer> getAllUnSuppressedBibId() throws Exception {      
      try {
         return catalogDao.getAllUnSuppressedBibId();
      } catch (Exception e) {
         throw e;
      }      
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getSuppressedBibId(java.lang.String, java.lang.String)
    */
   public List<String> getSuppressedBibId(String fromDateString, String toDateString) throws Exception {
      List<String> bibIdList;
      try {
         bibIdList = catalogDao.getSuppressedBibId(fromDateString, toDateString);
      } catch (Exception e) {
         throw e;
      }
      return bibIdList;
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAllSuppressedMfhdId()
    */
   public List<Integer> getAllSuppressedMfhdId() throws Exception {      
      try {
         return catalogDao.getAllSuppressedMfhdId();
      } catch (Exception e) {
         throw e;
      }      
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getAllSuppressedMfhdId()
    */
   public List<Integer> getAllUnSuppressedMfhdId() throws Exception {
      List<Integer> mfhdIdList;
      try {
         mfhdIdList = catalogDao.getAllUnSuppressedMfhdId();
      } catch (Exception e) {
         throw e;
      }
      return mfhdIdList;
   }
   
   /* (non-Javadoc)
    * @see edu.cornell.library.integration.service.CatalogService#getSuppressedMfhdId(java.lang.String, java.lang.String)
    */
   public List<String> getSuppressedMfhdId(String fromDateString, String toDateString) throws Exception {
      List<String> mfhdIdList;
      try {
         mfhdIdList = catalogDao.getSuppressedMfhdId(fromDateString, toDateString);
      } catch (Exception e) {
         throw e;
      }
      return mfhdIdList;
   }

   public BibMasterData getBibMasterData(String bibid) throws Exception {
      BibMasterData bibMasterData;
      try {
         bibMasterData = catalogDao.getBibMasterData(bibid);
      } catch (Exception e) {
         throw e;
      }
      return bibMasterData;
   }

   public MfhdMasterData getMfhdMasterData(String mfhdid) throws Exception {
      MfhdMasterData mfhdMasterData;
      try {
         mfhdMasterData = catalogDao.getMfhdMasterData(mfhdid);
      } catch (Exception e) {
         throw e;
      }
      return mfhdMasterData;
   }
    
   
}
