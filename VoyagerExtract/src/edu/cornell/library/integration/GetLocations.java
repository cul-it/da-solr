package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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

import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetLocations {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService;
   private IntegrationDataProperties integrationDataProperties;
   

   /**
    * default constructor
    */
   public GetLocations() { 
       
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
    * 
    */
   public void run() {
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

      if (ctx.containsBean("integrationDataProperties")) {
         setIntegrationDataProperties((IntegrationDataProperties) ctx.getBean("integrationDataProperties"));
      } else {
         System.err.println("Could not get integrationDataProperties");
         System.exit(-1);
      }    

       
      List<Location> locationList = new ArrayList<Location>();
      try {
         locationList = getCatalogService().getAllLocation();
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      //for (Location location: locationList) {
      //   ObjectUtils.printBusinessObject(location);
      //}
      LocationInfo locationInfo = new LocationInfo();
      locationInfo.setLocationList(locationList);
      String xml = getXml(locationInfo);
      
      try {
         saveXml(xml);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }   
      
   }


   /**
    * @param args
    */
   public static void main(String[] args) {
     GetLocations app = new GetLocations();
     app.run();
   }
   
   /**
    * @param locationInfo
    * @return
    */
   public String getXml(LocationInfo locationInfo) {
      // use xstream to rend the model into xml
      XStream xstream = new XStream(new DomDriver()); // does not require XPP3 library
      xstream.alias("location", Location.class);
      xstream.alias("locationInfo", LocationInfo.class);
      String xml = new String();

      xml += "<?xml version='1.0' encoding='UTF-8'?>\n";          
      xml += xstream.toXML(locationInfo) + "\n";
      return xml;
   }
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveXml(String xml) throws Exception {
      try {
         byte[] bytes = xml.getBytes("UTF-8");
         InputStream isr = new  ByteArrayInputStream(bytes);
         
         String url = getIntegrationDataProperties().getVoyagerLocationsXml() + "/locations.xml";      
         getDavService().saveFile(url, isr);
      } catch (Exception ex) {
         throw ex;
      }
   }
   
   public void saveXmlToFile(String xml) throws Exception {
      String fname = "/usr/local/src/integrationlayer/VoyagerExtract/locations.xml";
      File file = new File(fname);
      try {
         FileUtils.writeStringToFile(file, xml);
      } catch (IOException ex) {
         throw ex;
      }

   }
   
   

}
