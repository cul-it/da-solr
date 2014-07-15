package edu.cornell.library.integration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class ConvertMfhdDailyToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdDailyToXml() { 
       
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
     ConvertMfhdDailyToXml app = new ConvertMfhdDailyToXml();
     if (args.length != 2 ) {
        System.err.println("You must provide a src and destination Dir as arguments");
        System.exit(-1);
     }
     String srcDir  = args[0]; 
     String destDir  = args[1];
     app.run(srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      // get list of mfhdids updates using recent date String
      List<String> srcList = new ArrayList<String>();
      try {
         System.out.println("Getting list of mfhd marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      MrcToXmlConverter converter = new MrcToXmlConverter();
      converter.setSrcType("mfhd");
      converter.setExtractType("daily");
      converter.setSplitSize(10000);
      converter.setDestDir(destDir);
      // iterate over mrc files
//      int srcCounter = 1;
      if (srcList.size() == 0) {
         System.out.println("No Daily Marc files available to process");
      } else {
         for (String srcFile  : srcList) {
            System.out.println("Converting mfhd mrc file: "+ srcFile);
            try {
               converter.convertMrcToXml(davService, srcDir, srcFile);
            } catch (Exception e) {
               try {
                  System.out.println("Exception thrown. Could not convert file: "+ srcFile);
                  e.printStackTrace();
               } catch (Exception e1) { 
                  e1.printStackTrace();
               } 
            }
   		}
      }
      
   } 
   
   /**
    * @return
    */
   protected String getDateString() {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   Calendar now = Calendar.getInstance();
	   Calendar earlier = now;
	   earlier.add(Calendar.HOUR, -3);
	   String ds = df.format(earlier.getTime());
	   return ds;
   }   

}
