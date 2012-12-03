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

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetRecentMfhdData {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService;
   private IntegrationDataProperties integrationDataProperties;
   

   /**
    * default constructor
    */
   public GetRecentMfhdData() { 
       
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
    * @return the integrationDataProperties
    */
   public IntegrationDataProperties getIntegrationDataProperties() {
      return this.integrationDataProperties;
   }


   /**
    * @param integrationDataProperties the integrationDataProperties to set
    */
   public void setIntegrationDataProperties(
         IntegrationDataProperties integrationDataProperties) {
      this.integrationDataProperties = integrationDataProperties;
   } 
   
   /**
    * @param args
    */
   public static void main(String[] args) {
     GetRecentMfhdData app = new GetRecentMfhdData();
     if (args.length != 1 ) {
        System.err.println("You must provide a destination dir as an argument");
        System.exit(-1);
     }
     String destDir = args[0];
     app.run(destDir);
   }

   /**
    * 
    */
   public void run(String destDir) {
      System.out.println("Get Recent MfhdData");
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

       
      List<String> bibIdList = new ArrayList<String>();
      try {
         System.out.println("Getting recent bibids");
         bibIdList = getCatalogService().getRecentMfhdIds(getDateString());
         System.out.println("Found "+ bibIdList.size() +" bibids.");
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      for (String bibid: bibIdList) {
         try {            
            System.out.println("Getting bibRecord for bibid: "+bibid);
            MfhdBlob bibBlob = catalogService.getMfhdBlob(bibid);            
            saveMfhdData(bibBlob, destDir);
         } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
         }   
            
      }
      
   }


   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdData(MfhdBlob bibBlob, String destDir) throws Exception {
      try {
         String bibid = bibBlob.getMfhdId();
         
      } catch (Exception ex) {
         throw ex;
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
