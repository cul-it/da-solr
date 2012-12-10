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
 
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.MarcException;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext; 
import edu.cornell.library.integration.service.DavService; 

public class CreateBibMarcXmlIndex {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService; 
   private String dataDir;   

   /**
    * default constructor
    */
   public CreateBibMarcXmlIndex() { 
       
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
     CreateBibMarcXmlIndex app = new CreateBibMarcXmlIndex();
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
      
      StringBuffer sb = new StringBuffer();
      String outputUrl = this.getDataDir() + "/MANIFEST.txt";
      String fileUrl = new String();
      FileOutputStream fos = null;
      FileInputStream fis = null;
      try {            
         
         List<String> srcFiles = davService.getFileList(this.getDataDir());
         File tmpFile = createTempFile();
         fos = new FileOutputStream(tmpFile);
         
         for (String srcFile: srcFiles) {
            if (! srcFile.endsWith(".xml")) break;
            fileUrl = this.getDataDir() + "/" + srcFile;
            System.out.println("processing srcFile: "+ fileUrl);
            String line = new String();
            List<String> biblist = readFile(fileUrl) ;
            for (String bibid : biblist) {
               line = bibid + ":" + fileUrl + "\n";
               fos.write(line.getBytes("UTF-8"));
            }
         }
         fos.close();
         
         fis = new FileInputStream(tmpFile);
         davService.saveFile(outputUrl, fis);
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } finally {
         try {
            fis.close();
         } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }
      }
      
   }
   
   protected List<String> getBibsFromZippedFile(String srcFile) {
      
      List<String> biblist = new ArrayList<String>();
      InputStream zippedis = null;
      InputStream is = null;
      try {
         zippedis = davService.getFileAsInputStream(srcFile);
         is = new GZIPInputStream(zippedis);       
         MarcReader reader = new MarcXmlReader(is);
         while (reader.hasNext()) {
            Record record = reader.next();
            List<ControlField> controlFields = record.getControlFields();
            //System.out.println("Got this many controlfields: "+ controlFields.size());
            for (ControlField field: controlFields) {
               //System.out.println("   "+ field.getTag());
               if (field.getTag().equals("001")) {
                  //System.out.println(field.getData());
                  biblist.add(field.getData());
               }
            }
         }
         
      } catch (MarcException e) {
         // probably not a marc file...just ignore it.   
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } finally {
         try {
            is.close();
            zippedis.close();
         } catch (IOException e) {
            // do nothing
         }
      }
      return biblist;
   }
   
   protected List<String> readFile(String srcFile) { 
      List<String> biblist = new ArrayList<String>();
      InputStream is = null;
      try {
         is = davService.getFileAsInputStream(srcFile);               
         MarcReader reader = new MarcXmlReader(is);
         while (reader.hasNext()) {
            Record record = reader.next();
            List<ControlField> controlFields = record.getControlFields();
            //System.out.println("Got this many controlfields: "+ controlFields.size());
            for (ControlField field: controlFields) {
               //System.out.println("   "+ field.getTag());
               if (field.getTag().equals("001")) {
                  //System.out.println(field.getData());
                  biblist.add(field.getData());
               }
            }
         }
         
      } catch (MarcException e) {
         // probably not a marc file...just ignore it.
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } finally {
         try {
            is.close();
         } catch (IOException e) {
            // do nothing
         }
      }
      return biblist;
   }
   
   protected File createTempFile() throws IOException {       
      return File.createTempFile("MANIFEST", ".tmp");      
   }
}
