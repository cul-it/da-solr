package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Clob;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

 

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetRecentMfhdDataCount {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

    
   private CatalogService catalogService; 

   /**
    * default constructor
    */
   public GetRecentMfhdDataCount() { 
       
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
     GetRecentMfhdDataCount app = new GetRecentMfhdDataCount();      
     app.run();
   }

   /**
    * 
    */
   public void run() {
      System.out.println("Get Recent MfhdDataCount");
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      } 
       
      
      try {
         System.out.println("Getting recent mfhdids");
         String ds = getDateString();
         int count = getCatalogService().getRecentMfhdIdCount(ds);
         System.out.println("Number of MfhdIds since "+ ds +" = "+ count);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      
   } 
   
   protected String getDateString() {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Calendar now = Calendar.getInstance();
      Calendar earlier = now;
      earlier.add(Calendar.HOUR, -3);
      String ds = df.format(earlier.getTime());
      return ds;
   }
}
