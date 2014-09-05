package edu.cornell.library.integration;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class GetAllSuppressionsFromCatalog extends VoyagerToSolrStep {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   /**
    * default constructor
    */
   public GetAllSuppressionsFromCatalog() { 
       
   }  
   
      
   public static void main(String[] args) throws Exception {       
       new GetAllSuppressionsFromCatalog().run( args );
   }
   
   public void run(String[] args) throws Exception  {
                                       
       if ( getCatalogService() == null ){              
          System.err.println("Could not get catalogService");
          System.exit(-1);
       }            
       
       VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig( args );

       setDavService(DavServiceFactory.getDavService(config));
       
       getAllSuppressedBibId( config );
       getAllSuppressedMfhdId( config );
       getAllUnSuppressedBibId( config );
       getAllUnSuppressedMfhdId( config );
                            
       System.out.println("Success");             
      
   }
   
   private void getAllSuppressedBibId(VoyagerToSolrConfiguration config) throws Exception{
       List<Integer> ids;
       try {
          ids = getCatalogService().getAllSuppressedBibId();
       } catch (Exception e) {
          throw new Exception("Could not get the suppressed BibIDs from the catalog service",e );
       } 
   
       String url = config.getWebdavBaseUrl() + "/" + config.getDailyBibSuppressedDir() +  "/"  
               + config.getDailyBibSuppressedFilenamePrefix() + "-"+ getDateString() +".txt";
       
       try {          
           int size = ids.size();
          saveList(ids, url );
          ids=null;
          System.out.println( "Saved suppressed BIB IDs to " + url );
          System.out.println( "suppressed BIB ID count: " + size );
       } catch (Exception e) {
           throw new Exception("Could not save the suppressed BibIDs to " + url ,e );
       }
   }
   
   private void getAllSuppressedMfhdId( VoyagerToSolrConfiguration config) throws Exception{       
       List<Integer> ids ;
       try {
          ids = getCatalogService().getAllSuppressedMfhdId();
       } catch (Exception e) {
           throw new Exception("Could not get suppressed Mfhd IDs from catalog service",e);
       } 
       
       String url = config.getWebdavBaseUrl() + "/" + config.getDailyMfhdSuppressedDir() + "/"  
               + config.getDailyMfhdSuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       try {           
           int size = ids.size();
          saveList(ids, url);
          ids=null;
          System.out.println( "Saved suppressed MFHD IDs to " + url );
          System.out.println( "suppressed MFHID ID count: " + size );
       } catch (Exception e) {
           throw new Exception("Could not save suppressed Mfhd IDs to " +url ,e);
       }          
   }
   
   
   private void getAllUnSuppressedBibId( VoyagerToSolrConfiguration config) throws Exception{       
       List<Integer> ids;
       try {
          ids = getCatalogService().getAllUnSuppressedBibId();
       } catch (Exception e) {
           throw new Exception("Could not get unsuppressed Bib IDs from catalog service",e);
       } 

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyBibUnsuppressedDir() + "/" +  
           config.getDailyBibUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       try {
           int size = ids.size();
          saveList(ids, url);
          ids=null;
          System.out.println( "Saved unsuppressed BIB IDs to " + url );
          System.out.println( "unsuppressed BIB ID count: " + size );
       } catch (Exception e) {
           throw new Exception("Could not save unsuppressed Bib IDs to " + url ,e);
       }          
   }
   
   
   private void getAllUnSuppressedMfhdId(VoyagerToSolrConfiguration config) throws Exception{       
       List<Integer> ids;
       try {
          ids = getCatalogService().getAllUnSuppressedMfhdId();
       } catch (Exception e) {
           throw new Exception("Could not get unsuppressed Mfhd IDs from catalog service",e);
       } 

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyMfhdUnsuppressedDir() + "/" + 
            config.getDailyMfhdUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       try {
           int size = ids.size();
           saveList(ids, url);
           ids=null;
           System.out.println( "Saved unsuppressed MFHD IDs to " + url );
           System.out.println( "unsuppressed MFHD ID count: " + size);
       } catch (Exception e) {
           throw new Exception("Could not save unsuppressed Mfhd IDs to " + url ,e);
       }          
   }
     
   /**
    * Save the list to a temporary file and then send that file to the webdav destUrl.
    * 
    * We are using a temp file to avoid large heap sizes. 
    * 
    */
   private void saveList(List<Integer> bibIdList, String destUrl ) throws Exception {
       Path tmpFile = saveToTmpFile( bibIdList );       
       getDavService().saveFile(destUrl, Files.newInputStream(tmpFile, new OpenOption[]{}) );            
   }
   
   
   
   private Path saveToTmpFile( List<Integer> bibIdList ) throws IOException{
              
       final Path path = Files.createTempFile("GetAllSuppressionsFromCatalog-", ".tmp");                     
       path.toFile().deleteOnExit();

       Runtime.getRuntime().addShutdownHook(new Thread() {
         public void run() {
           try {
             Files.delete(path);
             System.out.println("deleted file at "+path);
           } catch (IOException e) {
             e.printStackTrace();
           }
         }
       });

     BufferedWriter out = Files.newBufferedWriter(path, Charset.forName("UTF-8"), new OpenOption[]{});
     try{
         for (Integer s : bibIdList) {
             out.write( s.toString() + "\n");         
         }    
     }finally{
         out.close();
     }
     
     return path;       
   }
   
  
   
   

}
