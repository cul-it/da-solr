package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;


import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.MarcException;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils;

public class MarcReader {
   
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   private DavService davService;

   public MarcReader() { 
       
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
   
   public void run() {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");  
       
      
      if (ctx.containsBean("davService")) {
         setDavService((DavService) ctx.getBean("davService"));
      } else {
         System.err.println("Could not get davService");
         System.exit(-1);
      } 
      
      String url = "http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.updates/5430043.mrc";
      String fname = "/tmp/test.mrc";
      try {
         Record record = null;
         //InputStream is = davService.getFileAsInputStream(url);
         FileInputStream is = new FileInputStream(new File(fname));
         MarcPermissiveStreamReader reader = null;
         boolean permissive      = true;
         boolean convertToUtf8   = false;
         reader = new MarcPermissiveStreamReader(is, permissive, convertToUtf8);
         
         while (reader.hasNext()) {
            try {
               record = reader.next();
            } catch (MarcException me) {
               System.out.println(me.getMessage());
               continue;
            } catch (Exception e) {
               e.printStackTrace();
               continue;
            }
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      
   }
   
   
   
   /**
    * @param args
    */
   public static void main(String[] args) {       
      MarcReader marcReader = new MarcReader();     
      marcReader.run();      
   }
   
   

}
