package edu.cornell.library.integration;

 
import java.io.ByteArrayInputStream;
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

public class ConvertBibToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertBibToXml() { 
       
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
     ConvertBibToXml app = new ConvertBibToXml();
     if (args.length != 3 ) {
        System.err.println("You must provide a bibid, src and destination Dir as arguments");
        System.exit(-1);
     }
     String bibid  = args[0];
     String srcDir  = args[1];
     String destDir  = args[2];
     app.run(bibid, srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String bibid, String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      ConvertUtils converter = new ConvertUtils();
      converter.setSrcType("bib");
      converter.setExtractType("single");
      converter.setSplitSize(0);
      converter.setSequence_prefix(bibid);
      try {            
          
         String srcFile = bibid + ".mrc";       
         converter.convertMrcToXml(davService, srcDir, srcFile); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
    
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveBibXml(String xml, String bibid, String destDir) throws Exception {
      String url = destDir + "/" + bibid +".xml";
      //System.out.println("Saving xml to: "+ url);
      try {         
         
         //FileUtils.writeStringToFile(new File("/tmp/test.mrc"), xml, "UTF-8");
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8"); 
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   } 
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   
    
}
