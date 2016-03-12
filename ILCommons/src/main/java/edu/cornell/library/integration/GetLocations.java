package edu.cornell.library.integration;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForWebdav;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

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
     GetLocations app = new GetLocations();
     List<String> requiredArgs = getRequiredArgsForWebdav();
     requiredArgs.add("locationDir");
     SolrBuildConfig config = SolrBuildConfig.loadConfig(args, requiredArgs);
     try {
		app.run(config);
	} catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
   }
   
   /**
 * @throws Exception 
    * 
    */
   public void run(SolrBuildConfig config) throws Exception {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      davService = DavServiceFactory.getDavService( config );

      List<Location> locationList = getCatalogService().getAllLocation();

      LocationInfo locationInfo = new LocationInfo();
      locationInfo.setLocationList(locationList);
      String xml = getXml(locationInfo);
      
      saveXml(xml, config.getWebdavBaseUrl()+"/"+config.getLocationDir() );
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
   public void saveXml(String xml, String destDir) throws Exception {
	   InputStream isr = IOUtils.toInputStream(xml, "UTF-8");
         
	   String url = destDir + "/locations.xml";
	   davService.saveFile(url, isr);
   }

}
