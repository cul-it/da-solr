package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class GetAllSuppressionsFromCatalog {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 

   /**
    * default constructor
    */
   public GetAllSuppressionsFromCatalog() { 
       
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
      return this.catalogService;
   }

   /**
    * @param catalogService the catalogService to set
    */
   public void setCatalogService(CatalogService catalogService) {
      this.catalogService = catalogService;
   } 
   
   public static void main(String[] args) {       
       new GetAllSuppressionsFromCatalog().run( args );
   }
   
   public void run(String[] args)  {
       try{
           ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
           
           if (ctx.containsBean("catalogService")) {
              setCatalogService((CatalogService) ctx.getBean("catalogService"));
           } else {
              System.err.println("Could not get catalogService");
              System.exit(-1);
           }

           setDavService(DavServiceFactory.getDavService()); 
           
           VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig( args );
           
           getAllSuppressedBibId( config );
           getAllSuppressedMfhdId( config );
           getAllUnSuppressedBibId( config );
           getAllUnSuppressedMfhdId( config );
                                
       }catch(Throwable th){
           th.printStackTrace();
           System.exit(1);
       }      
      
   }
   
   private void getAllSuppressedBibId(VoyagerToSolrConfiguration config) throws Exception{
       List<String> ids = new ArrayList<String>();
       try {
          ids = getCatalogService().getAllSuppressedBibId();
       } catch (Exception e) {
          throw new Exception("Could not get the suppressed BibIDs from the catalog service",e );
       } 
       
       try {
           String url = config.getWebdavBaseUrl() +  config.getDailySuppressedDir() 
                   +  "/suppressedBibId-"+ getDateString() +".txt";
          saveList(ids, url );
       } catch (Exception e) {
           throw new Exception("Could not save the suppressed BibIDs",e );
       }
   }
   
   private void getAllSuppressedMfhdId( VoyagerToSolrConfiguration config) throws Exception{       
       List<String> ids = new ArrayList<String>();
       try {
          ids = getCatalogService().getAllSuppressedMfhdId();
       } catch (Exception e) {
           throw new Exception("Could not get suppressed Mfhd IDs from catalog service",e);
       } 
       
       try {
           String url = config.getWebdavBaseUrl() + config.getDailySuppressedDir() 
                   + "/suppressedMfhdId-" + getDateString() + ".txt";
          saveList(ids, url);
       } catch (Exception e) {
           throw new Exception("Could not save suppressed Mfhd IDs",e);
       }          
   }
   
   
   private void getAllUnSuppressedBibId( VoyagerToSolrConfiguration config) throws Exception{       
       List<String> ids = new ArrayList<String>();
       try {
          ids = getCatalogService().getAllSuppressedMfhdId();
       } catch (Exception e) {
           throw new Exception("Could not get unsuppressed Bib IDs from catalog service",e);
       } 
       
       try {
           String url = config.getWebdavBaseUrl() + config.getDailyUnsuppressedDir() 
                   + "/unsuppressedBibid-" + getDateString() + ".txt";
          saveList(ids, url);
       } catch (Exception e) {
           throw new Exception("Could not save unsuppressed Bib IDs",e);
       }          
   }
   
   
   private void getAllUnSuppressedMfhdId(VoyagerToSolrConfiguration config) throws Exception{       
       List<String> ids = new ArrayList<String>();
       try {
          ids = getCatalogService().getAllSuppressedMfhdId();
       } catch (Exception e) {
           throw new Exception("Could not get unsuppressed Mfhd IDs from catalog service",e);
       } 
       
       try {
           String url = config.getWebdavBaseUrl() + config.getDailyUnsuppressedDir() 
                   + "/unsuppressedMfhdId-" + getDateString() + ".txt";
          saveList(ids, url);
       } catch (Exception e) {
           throw new Exception("Could not save unsuppressed Mfhd IDs",e);
       }          
   }
       
   private void saveList(List<String> bibIdList, String destUrl ) throws Exception {      
     StringBuilder sb = new StringBuilder();
     for (String s : bibIdList) {
         sb.append(s+"\n");
     }
     byte[] bytes = sb.toString().getBytes("UTF-8");
     InputStream isr = new  ByteArrayInputStream(bytes);
                
     getDavService().saveFile(destUrl, isr);      
   }   
   
   private String getDateString() {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      Calendar now = Calendar.getInstance();      
      String ds = df.format(now.getTime());
      return ds;
   }
   

}
