package edu.cornell.library.integration;

 
import java.text.SimpleDateFormat; 
import java.util.Calendar;

 

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

 
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetRecentBibDataCount {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

    
   private CatalogService catalogService; 

   /**
    * default constructor
    */
   public GetRecentBibDataCount() { 
       
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
     GetRecentBibDataCount app = new GetRecentBibDataCount();      
     app.run();
   }

   /**
    * 
    */
   public void run() {
      System.out.println("Get Recent BibDataCount");
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      } 
       
      
      try {
         String ds = getDateString();
         System.out.println("Getting updates since: "+ ds);
         int count = getCatalogService().getRecentBibIdCount(ds);
         System.out.println("Number of BibIds  = "+ count);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      
   } 
   
   protected String getDateString() {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Calendar now = Calendar.getInstance();
      Calendar earlier = now;
      earlier.add(Calendar.HOUR, -3);
      String ds = df.format(earlier.getTime());
      return ds;
   }
}
