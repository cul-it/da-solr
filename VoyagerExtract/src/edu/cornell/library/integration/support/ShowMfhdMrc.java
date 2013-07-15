package edu.cornell.library.integration.support;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException; 
import java.util.List;
 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import edu.cornell.library.integration.bo.MfhdData; 
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils;

public class ShowMfhdMrc {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
 
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ShowMfhdMrc() { 
       
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
     ShowMfhdMrc app = new ShowMfhdMrc();
     if (args.length != 1 ) {
        System.err.println("You must provide a mfhdid as an argument");
        System.exit(-1);
     }
     String mfhdid  = args[0]; 
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
      
      try {            
         System.out.println("Getting mfhd mrc for mfhdid: "+mfhdid);
         List<MfhdData>  mfhdDataList = catalogService.getMfhdData(mfhdid);
         StringBuffer sb = new StringBuffer();
         for (MfhdData mfhdData : mfhdDataList) {
            sb.append(mfhdData.getRecord());
         }
         ConvertUtils convert = new ConvertUtils();
         Record record = convert.getMarcRecord(sb.toString());
         System.out.println(record.toString()); 
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
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
