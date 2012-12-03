package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import oracle.sql.*;
import java.util.ArrayList;
import java.util.List;

 
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;
 
import edu.cornell.library.integration.bo.BibData; 
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertBibFull {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;    
   

   /**
    * default constructor
    */
   public ConvertBibFull() { 
       
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
    * @param args
    */
   public static void main(String[] args) {
     ConvertBibFull app = new ConvertBibFull();
     if (args.length != 2 ) {
        System.err.println("You must provide a src and  destination Dir as arguments");
        System.exit(-1);
     }
     String srcDir = args[0];
     String destDir  = args[1];
     app.run(srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      
      if (ctx.containsBean("davService")) {
         setDavService((DavService) ctx.getBean("davService"));
      } else {
         System.err.println("Could not get davService");
         System.exit(-1);
      } 
      
      try {            
         System.out.println("Converting Full Bib records" );
         List<String> fileList = this.davService.getFileList(srcDir);
         for (String filename: fileList) {
            //System.out.println("file: "+ s);
            convertBibData(filename, srcDir, destDir);
         }
         
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public void convertBibData(String filename, String srcDir, String destDir) throws Exception {
      
      Record record = null;
      MarcXmlWriter writer = null;
      System.out.println("srcFile: "+ srcDir +"/"+ filename);
      InputStream is = this.davService.getFileAsInputStream(srcDir +"/"+ filename);
      OutputStream ostream = null;
      
      
      try {
         String xml;
         MarcPermissiveStreamReader reader = null;
         boolean permissive      = true;
         boolean convertToUtf8   = true;
         reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
         ostream = new ByteArrayOutputStream();
         writer = new MarcXmlWriter(ostream, "UTF-8");
         
         int errorCount = 0;
         
         while (reader.hasNext()) {
            try {
               record = reader.next();
               writer.write(record);
               
            } catch (MarcException me) {
               System.out.println(me.getMessage());
               System.out.println("cause: "+ me.getCause());
               errorCount++;
               continue;
            } catch (Exception e) {
               e.printStackTrace();
               errorCount++;
               continue;
            }
         }
         
         xml = new String(ostream.toString());
         ostream.close();
         //System.out.println("xml: "+ xml);
         String destFile = StringUtils.replace(filename, ".mrc", ".xml");
         System.out.println("destFile: "+ destDir +"/"+ destFile);
         InputStream xmlInputStream = IOUtils.toInputStream(xml);
         this.davService.saveFile(destDir +"/"+ destFile, xmlInputStream);
         xmlInputStream.close();
         if (errorCount > 0 ) {
            throw new Exception("marc reader exception encountered - errors found:"+errorCount);
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } 
   }
      
   
    
}
