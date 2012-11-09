package edu.cornell.library.integration;

import info.extensiblecatalog.OAIToolkit.api.Importer;
import info.extensiblecatalog.OAIToolkit.importer.DirectoryNameGiver;
import info.extensiblecatalog.OAIToolkit.importer.ImporterConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.util.ObjectUtils;

public class Marc2MarcXML {
   
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   private ImporterConfiguration importerConfiguration;

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
   
   public void run() {
      //ObjectUtils.printBusinessObject(getImporterConfiguration());
      Importer importer = new Importer();
      importer.configuration = this.importerConfiguration;
      DirectoryNameGiver dirNameGiver = new DirectoryNameGiver(this.importerConfiguration);
      ObjectUtils.printBusinessObject(dirNameGiver);
      importer.setDirNameGiver(dirNameGiver);
      importer.execute();
   }
   
   public static void main(String[] args) {
      ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
      Marc2MarcXML marc2xml = new Marc2MarcXML();
      marc2xml.setImporterConfiguration( (ImporterConfiguration) ctx.getBean("importerConfiguration"));
      marc2xml.run();      
   }
   
   

}
