package edu.cornell.library.integration;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.service.CatalogService;

public class VoyagerToSolrStep {

    ApplicationContext ctx = null;
    private DavService davService;
    private CatalogService catalogService;

    public VoyagerToSolrStep() {
        super();
    }

    /**
        * @return the davService
        */
    public DavService getDavService() {
          return this.davService;
       }

    /**
        * @param davService the davService to set
        */
    public void setDavService(DavService davService) {
          this.davService = davService;
       }

    /**
        * @return the catalogService
        */
    public CatalogService getCatalogService() {
        if( this.catalogService == null ){            
            if (ctx.containsBean("catalogService")) {
                this.catalogService = ((CatalogService) ctx.getBean("catalogService"));
            } else {
                System.err.println("Could not get catalogService from context");
                System.exit(-1);
            }
        }            
            
        return this.catalogService;
    }

    /**
        * @param catalogService the catalogService to set
        */
    public void setCatalogService(CatalogService catalogService) {
          this.catalogService = catalogService;
       }

    protected String getDateString() {
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          Calendar now = Calendar.getInstance();      
          String ds = df.format(now.getTime());
          return ds;
       }
    
    protected ApplicationContext getContext(){
        if( ctx != null ){
            ctx = new ClassPathXmlApplicationContext("spring.xml");                   
        }
        return ctx;
    }

}