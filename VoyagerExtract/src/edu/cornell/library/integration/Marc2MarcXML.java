package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;

import info.extensiblecatalog.OAIToolkit.api.Importer;
import info.extensiblecatalog.OAIToolkit.importer.DirectoryNameGiver;
import info.extensiblecatalog.OAIToolkit.importer.ImporterConfiguration;

import org.apache.commons.io.FileUtils;
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
   
   public void run(String srcDir, String destDir) {
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

      /*if (ctx.containsBean("integrationDataProperties")) {
         setIntegrationDataProperties((IntegrationDataProperties) ctx.getBean("integrationDataProperties"));
      } else {
         System.err.println("Could not get integrationDataProperties");
         System.exit(-1);
      }   */ 
      
      
      Importer importer = new Importer();
      importer.configuration = this.importerConfiguration;
      ObjectUtils.printBusinessObject(importer.configuration);
      DirectoryNameGiver dirNameGiver = new DirectoryNameGiver(this.importerConfiguration);
      
      ObjectUtils.printBusinessObject(dirNameGiver);
      importer.setDirNameGiver(dirNameGiver);
     
      
      try {
         copyFromDav(srcDir,  importerConfiguration.getSourceDir());
         importer.execute();
         saveToDav(importerConfiguration.getDestinationDir(), destDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      
   }
   
   /**
    * @param srcDir
    * @param convertSrcDir
    * @throws Exception
    */
   public void copyFromDav(String srcDir, String convertSrcDir) throws Exception {
      List<String> fileList = getDavService().getFileList(srcDir);
      System.out.println("Destination dir: "+ convertSrcDir);
      for (String fname : fileList) {
         System.out.println("copying file from : "+ srcDir +"/"+ fname );
         String outputfile = convertSrcDir + "/" + fname;
         System.out.println("copying file to: "+ outputfile);
         InputStream inStream = getDavService().getFileAsInputStream(srcDir +"/"+ fname);
         FileOutputStream outStream = new FileOutputStream(new File(outputfile));
          

         int DEFAULT_BUFFER_SIZE = 1024;
         byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
         long count = 0;
         int n = 0;

         n = inStream.read(buffer, 0, DEFAULT_BUFFER_SIZE);

         while (n >= 0) {
            outStream.write(buffer, 0, n);
            n = inStream.read(buffer, 0, DEFAULT_BUFFER_SIZE);
         }
         
         inStream.close();
         outStream.close();
      }
   }
   
   /**
    * @param convertDestDir
    * @param destDir
    * @throws Exception
    */
   public void saveToDav(String convertDestDir, String destDir) throws Exception {
      
   }
   
   /**
    * @param args
    */
   public static void main(String[] args) {       
      Marc2MarcXML marc2xml = new Marc2MarcXML();
      if (args.length != 2 ) {
         System.err.println("You must provide a src Dir and dest Dir as  arguments");
         System.exit(-1);
      }
      String srcDir = args[0];
      String destDir = args[1];
      marc2xml.run(srcDir, destDir);      
   }
   
   

}
