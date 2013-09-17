package edu.cornell.library.integration;

 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.bo.MfhdMasterData;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.util.ObjectUtils;

public class GetMfhdMasterData {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
 
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetMfhdMasterData() { 
       
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
     GetMfhdMasterData app = new GetMfhdMasterData();
     if (args.length != 1 ) {
        System.err.println("You must provide a mfhdid as an argument");
        System.exit(-1);
     }
     String mfhdid = args[0];
     app.run(mfhdid);
   }
   
   /**
    * 
    */
   public void run(String mfhdid) {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

       
       
      MfhdMasterData mfhdMasterData = new MfhdMasterData();
      try {
         mfhdMasterData = getCatalogService().getMfhdMasterData(mfhdid);
         ObjectUtils.printBusinessObject(mfhdMasterData);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   

}
