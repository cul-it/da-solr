package edu.cornell.library.integration.service;

import java.util.ArrayList;
import java.util.List;


import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.dao.CatalogDao;
import edu.cornell.library.integration.service.CatalogService;

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
   
   public int getRecentMfhdIdCount(String dateString) throws Exception {
      
      try {
         return catalogDao.getRecentMfhdIdCount(dateString);
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
   
   public List<MfhdData> getMfhdData(String mfhdid) throws Exception {
      List<MfhdData> mfhdDataList = new ArrayList<MfhdData>();
      
      try {
         mfhdDataList = catalogDao.getMfhdData(mfhdid);
         return mfhdDataList;
         
      } catch (Exception e) {
         throw e;
      }
   }
    

}
