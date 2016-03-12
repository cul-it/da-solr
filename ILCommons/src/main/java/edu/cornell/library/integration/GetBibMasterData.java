package edu.cornell.library.integration;

 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.util.ObjectUtils;

public class GetBibMasterData {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
 
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetBibMasterData() { 
       
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
     GetBibMasterData app = new GetBibMasterData();
     if (args.length != 1 ) {
        System.err.println("You must provide a bibid as an argument");
        System.exit(-1);
     }
     String bibid = args[0];
     app.run(bibid);
   }
   
   /**
    * 
    */
   public void run(String bibid) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

       
       
      BibMasterData bibMasterData = new BibMasterData();
      try {
         bibMasterData = getCatalogService().getBibMasterData(bibid);
         ObjectUtils.printBusinessObject(bibMasterData);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   

}
