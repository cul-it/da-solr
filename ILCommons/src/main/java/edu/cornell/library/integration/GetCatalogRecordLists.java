package edu.cornell.library.integration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class GetCatalogRecordLists extends VoyagerToSolrStep {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   public static void main(String[] args) throws Exception {       
       new GetCatalogRecordLists(args);
   }

   public GetCatalogRecordLists(String[] args) throws Exception {

       if ( getCatalogService() == null ){              
          System.err.println("Could not get catalogService");
          System.exit(-1);
       }            

       SolrBuildConfig config = SolrBuildConfig.loadConfig( args );

       setDavService(DavServiceFactory.getDavService(config));

       getAllUnSuppressedBibs( config );
       getAllUnSuppressedMfhds( config );
       getAllItems( config );

       System.out.println("Success");             

   }

   private void getAllItems(SolrBuildConfig config) throws Exception {

	   Path tempFile = getTempFile();

	   int count = getCatalogService().saveAllItemMaps(tempFile);

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyItemDir() + "/" +  
               config.getDailyItemFilenamePrefix() + "-" + getDateString() + ".txt";

       getDavService().saveFile(url, Files.newInputStream(tempFile) );

       System.out.println( "Saved item information to " + url );
       System.out.println( "item count: " + count );
   }

   private void getAllUnSuppressedBibs( SolrBuildConfig config) throws Exception {  

	   Path tempFile = getTempFile();

	   int count = getCatalogService().saveAllUnSuppressedBibsWithDates(tempFile);

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyBibUnsuppressedDir() + "/" +  
           config.getDailyBibUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       getDavService().saveFile(url, Files.newInputStream(tempFile) );

       System.out.println( "Saved unsuppressed BIB IDs to " + url );
       System.out.println( "unsuppressed BIB ID count: " + count );
   }
   
   
   private void getAllUnSuppressedMfhds(SolrBuildConfig config) throws Exception{       

	   Path tempFile = getTempFile();

	   int count = getCatalogService().saveAllUnSuppressedMfhdsWithDates(tempFile);

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyMfhdUnsuppressedDir() + "/" + 
            config.getDailyMfhdUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       getDavService().saveFile(url, Files.newInputStream(tempFile) );

       System.out.println( "Saved unsuppressed MFHD IDs to " + url );
       System.out.println( "unsuppressed MFHD ID count: " + count);
   }

   private Path getTempFile() throws Exception {

	   final Path tempFile = Files.createTempFile("GetCatalogRecordLists-", ".tmp");                     
       tempFile.toFile().deleteOnExit();

       Runtime.getRuntime().addShutdownHook(new Thread() {
         public void run() {
           try {
             Files.delete(tempFile);
             System.out.println("deleted file at "+tempFile);
           } catch (IOException e) {
             e.printStackTrace();
           }
         }
       });

       return tempFile;
   }

}
