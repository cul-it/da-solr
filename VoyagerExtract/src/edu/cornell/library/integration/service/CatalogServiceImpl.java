package edu.cornell.library.integration.service;

import java.util.ArrayList;
import java.util.List;

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
    

}
