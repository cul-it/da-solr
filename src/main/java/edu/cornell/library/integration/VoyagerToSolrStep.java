package edu.cornell.library.integration;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;

public class VoyagerToSolrStep {

    ApplicationContext ctx = null;
    private DavService davService;

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

    protected String getDateString() {
          SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
          Calendar now = Calendar.getInstance();      
          String ds = df.format(now.getTime());
          return ds;
       }

    protected ApplicationContext getContext(){
        if( ctx == null ){
            ctx = new ClassPathXmlApplicationContext("spring.xml");
            if( ctx == null )
                throw new Error("could not load application context from spring.xml");
        }        
        return ctx;
    }

}