package edu.cornell.library.integration.service;

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
    

}
