package edu.cornell.library.integration;


import java.io.ByteArrayInputStream;
import java.io.InputStream; 
import java.io.UnsupportedEncodingException; 
import java.text.SimpleDateFormat; 
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List; 
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext; 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils;
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertMfhdFullToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdFullToXml() { 
       
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
     ConvertMfhdFullToXml app = new ConvertMfhdFullToXml();
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
      String badDir = srcDir +".bad";
      String doneDir = srcDir +".done";
      // get list of Full mrc files
      List<String> srcList = new ArrayList<String>();
      try {
         //System.out.println("Getting list of Mfhd marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      ConvertUtils converter = new ConvertUtils();
      converter.setSrcType("mfhd");
      converter.setExtractType("full");
      converter.setSplitSize(10000);
      converter.setDestDir(destDir);
      // iterate over mrc files
      if (srcList.size() == 0) {
         System.out.println("No Full Marc files available to process");
      } else {
         int seqno = 1;
         for (String srcFile  : srcList) {
            //System.out.println("Converting mrc file: "+ srcFile);
   			try {
   			              
               converter.setSequence_prefix(seqno);
               seqno++;
               String ts = getTimestampFromFileName(srcFile);
               converter.setTs(ts);
               InputStream is = davService.getFileAsInputStream(srcDir + "/" +srcFile);
               converter.convertMrcToXml(is, davService);
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
   public String getTimestampFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      if (tokens.length > 3) {
         return tokens[2] +"."+ tokens[3];   
      } else {
         return tokens[2];
      }
   }
   
   /**
    * @param srcFile
    * @return
    */
   public String getMfhdIdFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      return tokens[1];
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
