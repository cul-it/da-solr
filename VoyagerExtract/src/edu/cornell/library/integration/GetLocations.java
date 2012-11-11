package edu.cornell.library.integration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

 

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetLocations {
   
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   public GetLocations() { 
       
   }   
 
   
   public static void main(String[] args) {
      ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
      CatalogService catalogService = (CatalogService) ctx.getBean("catalogService");
      DavService davService = (DavService) ctx.getBean("davService");
      List<Location> locationList = new ArrayList<Location>();
      try {
         locationList = catalogService.getAllLocation();
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
      saveXml(xml);
      
   }
   
   public static String getXml(LocationInfo locationInfo) {
      // use xstream to rend the model into xml
      XStream xstream = new XStream(new DomDriver()); // does not require XPP3 library
      xstream.alias("location", Location.class);
      xstream.alias("locationInfo", LocationInfo.class);
      String xml = new String();

      xml += "<?xml version='1.0' encoding='UTF-8'?>\n";          
      xml += xstream.toXML(locationInfo) + "\n";
      return xml;
   }
   
   public static void saveXml(String xml) {
      String fname = "/usr/local/src/integrationlayer/VoyagerExtract/locations.xml";
      File file = new File(fname);
      try {
         FileUtils.writeStringToFile(file, xml);
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

   }
   
   

}
