package edu.cornell.library.integration;

import info.extensiblecatalog.OAIToolkit.api.Importer;
import info.extensiblecatalog.OAIToolkit.importer.DirectoryNameGiver;
import info.extensiblecatalog.OAIToolkit.importer.ImporterConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.util.ObjectUtils;

public class Marc2MarcXML {
   
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   private ImporterConfiguration importerConfiguration;
   private DavService davService;
   private IntegrationDataProperties integrationDataProperties;

   public Marc2MarcXML() { 
       
   }
   
    
   /**
    * @return the importerConfiguration
    */
   public ImporterConfiguration getImporterConfiguration() {
      return importerConfiguration;
   }

   /**
    * @param importerConfiguration the importerConfiguration to set
    */
   public void setImporterConfiguration(ImporterConfiguration importerConfiguration) {
      this.importerConfiguration = importerConfiguration;
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
   
   public void run() {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
      
      if (ctx.containsBean("importerConfiguration")) {
         setImporterConfiguration((ImporterConfiguration) ctx.getBean("importerConfiguration"));
      } else {
         System.err.println("Could not get importerConfiguration");
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
      
      Importer importer = new Importer();
      importer.configuration = this.importerConfiguration;
      DirectoryNameGiver dirNameGiver = new DirectoryNameGiver(this.importerConfiguration);
      ObjectUtils.printBusinessObject(dirNameGiver);
      importer.setDirNameGiver(dirNameGiver);
      importer.execute();
   }
   
   public static void main(String[] args) {       
      Marc2MarcXML marc2xml = new Marc2MarcXML();       
      marc2xml.run();      
   }
   
   

}
