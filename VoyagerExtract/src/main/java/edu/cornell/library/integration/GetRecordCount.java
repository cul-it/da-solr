package edu.cornell.library.integration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.marc.Record;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
public class GetRecordCount {
   
   protected final Log logger = LogFactory.getLog(getClass());
   public static final String TMPDIR = "/tmp";
   
   private DavService davService;

   public GetRecordCount() {
      // TODO Auto-generated constructor stub
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
   
   public int countRecords(DavService davService, String srcDir, String srcFile) throws Exception {
      int total = 0;    
      @SuppressWarnings("unused")
      Record record = null;      
      //InputStream is = davService.getFileAsInputStream(srcDir + "/" +srcFile);
      String tmpFilePath = TMPDIR +"/"+ srcFile;
      File f = davService.getFile(srcDir +"/"+ srcFile, tmpFilePath);
      FileInputStream is = new FileInputStream(f);
      MarcPermissiveStreamReader reader = null;
      boolean permissive      = true;
      boolean convertToUtf8   = true;
      reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8); 
       
      
      while (reader.hasNext()) {
         try {
            record = reader.next();
            total++;
         } catch (MarcException me) {
            logger.error("MarcException reading record", me);
            continue;
         } catch (Exception e) {
            e.printStackTrace();
            continue;
         } 
          
      } // end while loop
    
       
      try { 
         is.close();
      } catch (IOException e) {
         e.printStackTrace();
      } 
       
      return total;
   }
   
public void run(String srcDir) {
      setDavService(DavServiceFactory.getDavService());       
      
      // get list of Full mrc files
      List<String> srcList = new ArrayList<String>();
      try {
         //System.out.println("Getting list of bib marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
        e.printStackTrace();
      } 
      if (srcList.size() == 0) {
         System.out.println("No Marc files available to process");
      } else { 
         for (String srcFile  : srcList) {
            //System.out.println("Converting mrc file: "+ srcFile);
            try { 
               int total = countRecords(davService, srcDir, srcFile);
               System.out.println(srcFile+ ": "+total);
            } catch (Exception e) {
               try {
                  System.out.println("Exception thrown. Could not read records: "+ srcFile);
                  e.printStackTrace();
                  
               } catch (Exception e1) { 
                  e1.printStackTrace();
               } 
            }
         }
      }
      
   }
   
   public static void main(String[] args) {
      GetRecordCount app = new GetRecordCount();
      if (args.length != 1 ) {
         System.err.println("You must provide a src Dir as an argument");
         System.exit(-1);
      }
      String srcDir  = args[0]; 
      app.run(srcDir);
    }

}
