package edu.cornell.library.integration;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import org.apache.commons.io.IOUtils; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils; 

public class ConvertMfhdToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdToXml() { 
       
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
     ConvertMfhdToXml app = new ConvertMfhdToXml();
     if (args.length != 3 ) {
        System.err.println("You must provide a mfhdid and  destination Dir as arguments");
        System.exit(-1);
     }
     String mfhdid  = args[0];
     String srcDir  = args[1];
     String destDir  = args[2];
     app.run(mfhdid, srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String mfhdid, String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      ConvertUtils converter = new ConvertUtils();
      converter.setSrcType("mfhd");
      converter.setExtractType("single");
      converter.setSplitSize(0);
      converter.setSequence_prefix(Integer.parseInt(mfhdid));
      try {            
         System.out.println("Getting mfhd mrc for mfhd id: "+mfhdid);
         String mrc = davService.getFileAsString(srcDir + "/" + mfhdid + ".mrc"); 
         InputStream is = davService.getFileAsInputStream(srcDir + "/" + mfhdid + ".mrc");
         converter.convertMrcToXml(is, davService); 
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   } 
    
}
