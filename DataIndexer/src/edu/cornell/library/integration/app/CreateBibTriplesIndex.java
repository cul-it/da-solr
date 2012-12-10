package edu.cornell.library.integration.app;

 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
 
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;  
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext; 

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory; 
import com.hp.hpl.jena.tdb.TDBFactory;
 

import edu.cornell.library.integration.service.DavService; 

public class CreateBibTriplesIndex {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService; 
   private String dataDir; 
   private final String dataNs = "http://culdata.library.cornell.edu/canonical/0.1/";
   private final String dataDevNs = "http://culdatadev.library.cornell.edu/canonical/0.1/";
   private final String bibIndexFileName = "/usr/local/src/integrationlayer/DataIndexer/bibIndexfile.nt";
   private final String uriNs = "http://fbw4-dev.library.cornell.edu/individuals";
   private final String uriDevNs = "http://fbw4-dev.library.cornell.edu/individuals";
   
   /**
    * default constructor
    */
   public CreateBibTriplesIndex() { 
       
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
    * @return the dataDir
    */
   public String getDataDir() {
      return this.dataDir;
   }

   /**
    * @param dataDir the dataDir to set
    */
   public void setDataDir(String dataDir) {
      this.dataDir = dataDir;
   } 


   /**
    * @param args
    */
   public static void main(String[] args) {
     CreateBibTriplesIndex app = new CreateBibTriplesIndex();
     if (args.length != 1 ) {
        System.err.println("You must provide a dataDir argument");
        System.exit(-1);
     }
     app.setDataDir(args[0]); 
     app.run();
   }
   

   /**
    * 
    */
   public void run() {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml"); 
      

      if (ctx.containsBean("davService")) {
         setDavService((DavService) ctx.getBean("davService"));
      } else {
         System.err.println("Could not get davService");
         System.exit(-1);
      }   
      String fileUrl = new String(); 
      
      File bibIndexFile  = new File(bibIndexFileName);
      FileUtils.deleteQuietly(bibIndexFile);
      
      try { 
         List<String> srcFiles = davService.getFileList(this.getDataDir()); 
         int count = 1000;
         for (String srcFile: srcFiles) {
            
            fileUrl = this.getDataDir() + "/" + srcFile;
            InputStream is = getSrcInputStream(fileUrl);
            if (is != null) {
               System.out.println("processing srcFile: "+ fileUrl);
               String fileObjectUri = "<"+ uriNs +"/bf" + count +"> ";
               addFileRefTriple(bibIndexFile, fileObjectUri, fileUrl);
               extractStatements(is, bibIndexFile, fileObjectUri) ;
               count++;
            } 
         } 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } finally {
         //
      } 
      
   }
   
   protected void extractStatements(InputStream is, File file, String fileObjectUri) {      
       
      // we are looking for something like this
      // <http://fbw4-dev.library.cornell.edu/individuals/b4040001> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> .

      final String bibObject = "<http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord>"; 
      
      BufferedReader in = null; 
      try { 
         in = new BufferedReader(new InputStreamReader(is));         

         String line;
         String bibSubj;
         String hasFileStatement;
         
         while ((line = in.readLine()) != null) {
            if (StringUtils.contains(line, bibObject)) {               
               FileUtils.writeStringToFile(file, line + "\n", true);
               String parts[] = StringUtils.split(line);
               bibSubj= parts[0];
               hasFileStatement = bibSubj +   " <"+ dataNs + "hasFile> " + fileObjectUri +" .\n";
               FileUtils.writeStringToFile(file, hasFileStatement, true);
            } 
         } 
         
      }  catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } finally {
         try {
            is.close(); 
         } catch (IOException e) {
            // do nothing
         }
      } 
   } 
   
   protected InputStream getSrcInputStream(String fileUrl)  {
      System.out.println("Getting inputstream from: "+fileUrl);
      try {
         if (fileUrl.endsWith(".nt")) {
            return davService.getFileAsInputStream(fileUrl);
         } else if (fileUrl.endsWith(".gz")) {
            InputStream zippedis = davService.getFileAsInputStream(fileUrl);
            return new GZIPInputStream(zippedis);
         } else {
            System.out.println("Unrecognized file type");
            return null;
         }
      } catch (Exception ex) {
         return null;
      }
   }
   
   protected void addFileRefTriple(File file, String subj, String srcFile ) {
      
      String pred = "<http://www.w3.org/2000/01/rdf-schema#label>";
      String statement = subj + " " + pred + " " + "\"" + srcFile + "\" .\n";
      System.out.println(statement);
      try {
         FileUtils.writeStringToFile(file, statement, true);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   } 
}
