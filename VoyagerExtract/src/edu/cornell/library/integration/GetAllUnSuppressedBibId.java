package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

 

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory; 

public class GetAllUnSuppressedBibId {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 

   /**
    * default constructor
    */
   public GetAllUnSuppressedBibId() { 
       
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
     GetAllUnSuppressedBibId app = new GetAllUnSuppressedBibId();
     if (args.length != 1 ) {
        System.err.println("You must provide destination dir as an argument");
        System.exit(-1);
     }
     String destDir = args[0];
     app.run(destDir);
   }
   
   /**
    * 
    */
   public void run(String destDir) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService()); 
       
      List<String> bibIdList = new ArrayList<String>();
      try {
         bibIdList = getCatalogService().getAllUnSuppressedBibId();
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
      try {
         saveList(bibIdList, destDir);
         //saveListToFile(bibIdList);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }   
      
   }
   
    
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveList(List<String> bibIdList, String destDir) throws Exception {
      try {
         StringBuilder sb = new StringBuilder();
         for (String s : bibIdList) {
             sb.append(s+"\n");
         }
         byte[] bytes = sb.toString().getBytes("UTF-8");
         InputStream isr = new  ByteArrayInputStream(bytes);
         
         String url = destDir + "/unsuppressedBibId.txt";      
         getDavService().saveFile(url, isr);
      } catch (Exception ex) {
         throw ex;
      }
   }
   
   public void saveListToFile(List<String> bibIdList) throws Exception {
      String fname = "/usr/local/src/integrationlayer/VoyagerExtract/unsuppressedBibid-"+ getDateString() +".txt";
      File file = new File(fname);
       
      StringBuilder sb = new StringBuilder();
      for (String s : bibIdList) {
          sb.append(s+"\n");
      }
       
      try {
         FileUtils.writeStringToFile(file, sb.toString());
      } catch (IOException ex) {
         throw ex;
      }

   }
   
   protected String getDateString() {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      Calendar now = Calendar.getInstance();      
      String ds = df.format(now.getTime());
      return ds;
   }
   
   

}
