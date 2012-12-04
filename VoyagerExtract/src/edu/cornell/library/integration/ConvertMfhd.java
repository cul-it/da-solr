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

public class ConvertMfhd {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhd() { 
       
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

   
   
   
   /**
    * @param args
    */
   public static void main(String[] args) {
     ConvertMfhd app = new ConvertMfhd();
     if (args.length != 2 ) {
        System.err.println("You must provide a mfhdid and  destination Dir as arguments");
        System.exit(-1);
     }
     String mfhdid  = args[0];
     String destDir  = args[1];
     app.run(mfhdid, destDir);
   }
   

   /**
    * 
    */
   public void run(String mfhdid, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      if (ctx.containsBean("davService")) {
         setDavService((DavService) ctx.getBean("davService"));
      } else {
         System.err.println("Could not get davService");
         System.exit(-1);
      } 
      
      try {            
         System.out.println("Getting mfhdRecord for mfhdid: "+mfhdid);
         List<MfhdData>  mfhdDataList = catalogService.getMfhdData(mfhdid);
         StringBuffer sb = new StringBuffer();
         for (MfhdData mfhdData : mfhdDataList) {
            sb.append(mfhdData.getRecord());
         }
         
         String mrc = sb.toString(); 
         System.out.println("mrc: "+mrc);
         String xml = convertMfhdData(mrc);
         System.out.println("xml: "+ xml);
         saveMfhdData(xml, mfhdid, destDir); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public String convertMfhdData(String mfhdStr) throws Exception {
      String xml = new String();
      char chr = mfhdStr.charAt(mfhdStr.length() - 1);
      System.out.println("terminator: "+ (int) chr); 
      String reclenstr = mfhdStr.substring(0,5); 
      System.out.println("reclen: "+ reclenstr);
      System.out.println("mfhdStrlen: "+ mfhdStr.length());

      Record record = null;
      MarcXmlWriter writer = null;
      InputStream is = stringToInputStream(mfhdStr);
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

   public void convertMfhdBlob(MfhdBlob mfhdBlob, String destDir) throws Exception {
      String xml = null;
      CLOB clob =(CLOB) mfhdBlob.getClob();
      
      Record record = null;
      MarcXmlWriter writer = null;
      InputStream is = null;
      OutputStream ostream = null;
      
      try {
          
         String mfhdStr = convertClobToString(clob);
         
         is = stringToInputStream(mfhdStr);
         MarcPermissiveStreamReader reader = null;
         boolean permissive      = true;
         boolean convertToUtf8   = true;
         reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
         ostream = new ByteArrayOutputStream();
         writer = new MarcXmlWriter(ostream, "UTF-8");
         
         int errorCount = 0;
         
         while (reader.hasNext()) {
            try {
               System.out.println("Reading record...");
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
         
         writer.close();

         xml = new String(ostream.toString());
         saveMfhdData(xml, mfhdBlob.getMfhdId(), destDir);
         if (errorCount > 0 ) {
            throw new Exception("marc reader exception encountered - errors found:"+errorCount);
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } 
   }
      
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdData(String xml, String mfhdid, String destDir) throws Exception {
      try {         
         
         FileUtils.writeStringToFile(new File("/tmp/test.mrc"), xml, "UTF-8");
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8");       
         
         String url = destDir + "/" + mfhdid +".mrc";      
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   }
   
   protected static String convertClobToString(CLOB clob)  { 
      
      try {        
         //System.out.println("clob length: "+ clob.length()); 
         char clobVal[] = new char[(int) clob.length()];
         char[]  reclen = new char[(int) 5];
         clob.getChars(0L, (int) 5, reclen);
          
         Reader r = clob.getCharacterStream();
         r.read(clobVal);         
         r.close();
         String record_length = new String(clobVal, 0, 5); 
         //System.out.println("record_length "+ record_length);
         StringWriter sw = new StringWriter();         
         sw.write(clobVal);        
         String mfhdStr = StringUtils.stripEnd(sw.toString(), " ");
         sw.close();
         return mfhdStr;
      } catch(Exception e) { 
         e.printStackTrace();
         return "";
      }   
      
   }
   
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   
    
}
