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

import edu.cornell.library.integration.bo.IdWithDate;
import edu.cornell.library.integration.bo.ItemMap;
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
	   List<ItemMap> items = getCatalogService().getAllItemMaps();
       String url = config.getWebdavBaseUrl() + "/" +config.getDailyItemDir() + "/" +  
               config.getDailyItemFilenamePrefix() + "-" + getDateString() + ".txt";

       int size = items.size();
       saveList(items, url);
       items=null;
       System.out.println( "Saved item IDs to " + url );
       System.out.println( "item ID count: " + size );
   }

   private void getAllUnSuppressedBibs( SolrBuildConfig config) throws Exception {  
	   List<IdWithDate> bibs = getCatalogService().getAllUnSuppressedBibsWithDates();

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyBibUnsuppressedDir() + "/" +  
           config.getDailyBibUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       int size = bibs.size();
       saveList(bibs, url);
       bibs=null;
       System.out.println( "Saved unsuppressed BIB IDs to " + url );
       System.out.println( "unsuppressed BIB ID count: " + size );
   }
   
   
   private void getAllUnSuppressedMfhds(SolrBuildConfig config) throws Exception{       
	   List<IdWithDate> mfhds = getCatalogService().getAllUnSuppressedMfhdsWithDates();

       String url = config.getWebdavBaseUrl() + "/" +config.getDailyMfhdUnsuppressedDir() + "/" + 
            config.getDailyMfhdUnsuppressedFilenamePrefix() + "-" + getDateString() + ".txt";
       
       int size = mfhds.size();
       saveList(mfhds, url);
       mfhds=null;
       System.out.println( "Saved unsuppressed MFHD IDs to " + url );
       System.out.println( "unsuppressed MFHD ID count: " + size);
   }

   private <T> void saveList(List<T> list, String destUrl ) throws Exception {

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

       BufferedWriter out = Files.newBufferedWriter(tempFile, Charset.forName("UTF-8"), new OpenOption[]{});
       try{
    	   for (Object s : list)
    		   out.write( s.toString() + "\n");         
       } finally{
    	   out.close();
       }
       getDavService().saveFile(destUrl, Files.newInputStream(tempFile, new OpenOption[]{}) );            
   }

}
