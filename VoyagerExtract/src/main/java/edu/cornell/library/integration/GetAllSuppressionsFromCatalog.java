package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class GetAllSuppressionsFromCatalog extends VoyagerToSolrStep {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   /**
    * default constructor
    */
   public GetAllSuppressionsFromCatalog() { 
       
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
           
           VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig( args );
           
           setDavService(DavServiceFactory.getDavService(config));
           
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
           String url = config.getWebdavBaseUrl() +  config.getDailyBibSuppressedDir() 
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
           String url = config.getWebdavBaseUrl() + config.getDailyMfhdSuppressedDir() 
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
           String url = config.getWebdavBaseUrl() + config.getDailyBibUnsuppressedDir() 
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
           String url = config.getWebdavBaseUrl() + config.getDailyMfhdUnsuppressedDir() 
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
   

}
