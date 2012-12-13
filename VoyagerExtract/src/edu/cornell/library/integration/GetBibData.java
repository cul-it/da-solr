package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import oracle.sql.*;
import java.util.ArrayList;
import java.util.List;

 
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
 
import edu.cornell.library.integration.bo.BibBlob; 
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetBibData {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService;
    
   

   /**
    * default constructor
    */
   public GetBibData() { 
       
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
     GetBibData app = new GetBibData();
     if (args.length != 2 ) {
        System.err.println("You must provide a bibid and destination Dir as arguments");
        System.exit(-1);
     }
     String bibid  = args[0];
     String destDir  = args[1];
     app.run(bibid, destDir);
   }
   

   /**
    * 
    */
   public void run(String bibid, String destDir) {
      System.out.println("Get BibData");
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      
      try {            
         System.out.println("Getting bibRecord for bibid: "+bibid);
         BibBlob bibBlob = catalogService.getBibBlob(bibid);        
         // ObjectUtils.printBusinessObject(bibBlob);
         saveBibBlob(bibBlob, destDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   } 
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveBibBlob(BibBlob bibBlob, String destDir) throws Exception {
      try {
         String bibid = bibBlob.getBibId();
         
         Clob clob = bibBlob.getClob();
          
         char[] chars = new char[(int) clob.length()];         
         Reader reader = clob.getCharacterStream();
         
         reader.read(chars);
         CharArrayWriter caw = new CharArrayWriter();
         caw.write(chars, 0, (int) clob.length());
         String str = caw.toString();
         FileOutputStream ostream = new FileOutputStream(new File("/tmp/test.mrc"));
         ostream.write(str.getBytes()); 
         ostream.close();
         
         FileUtils.writeStringToFile(new File("/tmp/test.mrc"), str, "UTF-8");
         InputStream isr = IOUtils.toInputStream(str, "UTF-8");       
         
         String url = destDir + "/" + bibid +".mrc";      
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   }
   
    
}
