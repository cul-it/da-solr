package edu.cornell.library.integration;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.util.ConvertUtils;
public class ConvertMarcToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMarcToXml() { 
       
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
     ConvertMarcToXml app = new ConvertMarcToXml();
     if (args.length != 4 ) {
        System.err.println("You must provide a src and destination Dir as arguments");
        System.exit(-1);
     }
     String srcType = args[0];
     String extractType = args[1];
     String srcDir  = args[2]; 
     String destDir  = args[3];
     app.run(srcType, extractType, srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String srcType, String extractType, String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      String badDir = srcDir +".bad";
      String doneDir = srcDir +".done";
      
      // get list of daily mrc files
      List<String> srcList = new ArrayList<String>();
      try {
         //System.out.println("Getting list of marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      ConvertUtils converter = new ConvertUtils();
      converter.setSrcType(srcType);
      converter.setExtractType(extractType);
      if (extractType.equals("updates")) {
         converter.setSplitSize(0);          
      } else {
         converter.setSplitSize(10000);
      }
      converter.setDestDir(destDir);
      // iterate over mrc files
      if (srcList.size() == 0) {
         System.out.println("No Marc files available to process");
      } else {
         String seqno = "";
         for (String srcFile  : srcList) {
            System.out.println("Converting mrc file: "+ srcFile);
   			try {
   			              
   			   seqno = getSequenceFromFileName(srcFile, extractType);
   			   //System.out.println("seqno: "+seqno);
               converter.setSequence_prefix(seqno);
               String ts = getTimestampFromFileName(srcFile, extractType);
               //System.out.println("ts: "+ts);
               converter.setTs(ts);
               if (extractType.equals("updates")){
                  converter.setItemId(seqno);
               }
   			   InputStream is = davService.getFileAsInputStream(srcDir + "/" +srcFile);
   				converter.convertMrcToXml(davService, srcDir, srcFile);
   				davService.moveFile(srcDir +"/" +srcFile, doneDir +"/"+ srcFile);
   			} catch (Exception e) {
   			   try {
                  System.out.println("Exception thrown. Could not convert file: "+ srcFile);
                  e.printStackTrace();
                  davService.moveFile(srcDir +"/" +srcFile, badDir +"/"+ srcFile);
               } catch (Exception e1) { 
                  e1.printStackTrace();
               } 
   			}
   		}
      }
      
   }
   
    
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected  InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);   
   }
   
   /**
    * @param srcFile
    * @return
    */
   public String getTimestampFromFileName(String srcFile, String extractType) {
      String[] tokens = StringUtils.split(srcFile, ".");
      if (extractType.equals("updates")) {
         return tokens[2];
      } else {
         return tokens[1];
      }

   }
   
   /**
    * @param srcFile
    * @return
    */
   public String getSequenceFromFileName(String srcFile, String extractType) {
      
      String[] tokens = StringUtils.split(srcFile, ".");
      if (extractType.equals("updates")) {
         return tokens[1];
      } else if (extractType.equals("daily"))  {
         return tokens[2]+"_"+tokens[3];
      } else {
         return tokens[2];   
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
