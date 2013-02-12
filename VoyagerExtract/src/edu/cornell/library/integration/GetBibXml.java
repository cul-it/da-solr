package edu.cornell.library.integration;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import edu.cornell.library.integration.bo.BibData; 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils; 

public class GetBibXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetBibXml() { 
       
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
     GetBibXml app = new GetBibXml();
     if (args.length != 2 ) {
        System.err.println("You must provide a bibid and  destination Dir as arguments");
        System.exit(-1);
     }
     String bibid  = args[0];
     String destDir  = args[1];
     app.run(bibid, destDir);
   }
   

   /**
    * 
    */
   public void run(String bibid, String destDir) {
      
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
      try {            
         System.out.println("Getting bib record for bibid: "+bibid);
         List<BibData>  bibDataList = catalogService.getBibData(bibid);
         StringBuffer sb = new StringBuffer();
         for (BibData bibData : bibDataList) {
            sb.append(bibData.getRecord());
         }
         
         String mrc = sb.toString(); 
         //System.out.println("mrc: "+mrc);
         converter.convertMrcToXml(mrc, davService);
         
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   } 
}
