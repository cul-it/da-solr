package edu.cornell.library.integration.service;

import java.util.ArrayList;
import java.util.List;


import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.Location;
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
    
   public List<Location> getAllLocation() throws Exception {
      List<Location> locationList;
      try {
         locationList = catalogDao.getAllLocation();
      } catch (Exception e) {
         throw e;
      }
      return locationList;

   }

   public List<String> getRecentBibIds() throws Exception {
      List<String> bibIdList;
      try {
         bibIdList = catalogDao.getRecentBibIds();
      } catch (Exception e) {
         throw e;
      }
      return bibIdList;
   }

   public BibData getBibData(String bibid) throws Exception {
      BibData bibData = new BibData();
      
      try {
         bibData = (BibData) catalogDao.getBibData(bibid);
         return bibData;
         
      } catch (Exception e) {
         throw e;
      }
   }
    

}
