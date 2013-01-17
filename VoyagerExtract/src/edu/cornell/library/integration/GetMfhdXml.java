package edu.cornell.library.integration;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.io.IOUtils; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import edu.cornell.library.integration.bo.MfhdData; 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils; 

public class GetMfhdXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetMfhdXml() { 
       
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
     GetMfhdXml app = new GetMfhdXml();
     if (args.length != 2 ) {
        System.err.println("You must provide a mfhdid and  destination Dir as arguments");
        System.exit(-1);
     }
     String mfhdid  = args[0];
     String destDir  = args[1];
     app.run(mfhdid, destDir);
   }
   

   /**
    * 
    */
   public void run(String mfhdid, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      ConvertUtils converter = new ConvertUtils();
      
      try {            
         System.out.println("Getting mfhdRecord for mfhdid: "+mfhdid);
         List<MfhdData>  mfhdDataList = catalogService.getMfhdData(mfhdid);
         StringBuffer sb = new StringBuffer();
         for (MfhdData mfhdData : mfhdDataList) {
            sb.append(mfhdData.getRecord());
         }
         
         String mrc = sb.toString();
         String xml = converter.convertMrcToXml(mrc);
         saveMfhdXml(xml, mfhdid, destDir); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
 
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdXml(String xml, String mfhdid, String destDir) throws Exception {
      String url = destDir + "/" + mfhdid +".xml";
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
}
