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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
 
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.MfhdData; 
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class Marc2MarcXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public Marc2MarcXml() { 
       
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
     Marc2MarcXml app = new Marc2MarcXml();
     if (args.length != 2 ) {
        System.err.println("You must provide a src and destination Dir as arguments");
        System.exit(-1);
     }
     String srcDir  = args[0];
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
         System.out.println("Getting src files...");
         List<String> srcFiles = davService.getFileList(srcDir);
         for (String srcFile: srcFiles) {
            String xml = convert(srcFile);
            System.out.println(StringUtils.substring(xml, 0, 100));
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public String convert(String srcFile) throws Exception {
      String xml = new String();
       
      Record record = null;
      MarcXmlWriter writer = null;
      InputStream is = stringToInputStream(xml);
      OutputStream ostream = null;
      try {
         
         boolean permissive      = true;
         boolean convertToUtf8   = true;

         MarcPermissiveStreamReader reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8, "UTF-8");
         ostream = new ByteArrayOutputStream();
         writer = new MarcXmlWriter(ostream, "UTF-8");
         
         int errorCount = 0;
         
         while (reader.hasNext()) {
            try {
               record = reader.next();
               System.out.println("record type: "+record.getType());
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
         
         if (errorCount > 0 ) {
            throw new Exception("marc reader exception encountered - errors found:"+errorCount);
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } finally {
         ostream.close();
      } 
      return xml;

   }

   
      
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveAsXml(String xml, String fname, String destDir) throws Exception {
      try {         
         String ofile = StringUtils.replace(fname, ".mrc", ".xml");
          
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8");       
         
         String url = destDir + "/" + ofile;      
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   }
   
   
   
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   
    
}
