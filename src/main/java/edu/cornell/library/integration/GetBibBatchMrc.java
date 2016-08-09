package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class GetBibBatchMrc {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetBibBatchMrc() { 
       
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
     GetBibBatchMrc app = new GetBibBatchMrc();
     if (args.length != 2 ) {
        System.err.println("You must provide a filename and  destination Dir as arguments");
        System.exit(-1);
     }
     String filename  = args[0];
     String destDir  = args[1];
     try {
		app.run(filename, destDir);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
   

   /**
    * 
    */
   public void run(String filename, String destDir) throws Exception {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      String biblist = FileUtils.readFileToString(new File(filename));
      //String[] bibArray = StringUtils.split(biblist, ",");
      String bibid = new String();
      StringTokenizer st = new StringTokenizer(biblist, ",");
      while (st.hasMoreElements()) {
    	 bibid = (String) st.nextToken().trim();
      	 if (StringUtils.isNotEmpty(bibid)) { 
            try {            
               //System.out.println("Getting bib mrc for bibid: "+bibid);
               List<BibData>  bibDataList = catalogService.getBibData(bibid);
               StringBuffer sb = new StringBuffer();
               for (BibData bibData : bibDataList) {
                  sb.append(bibData.getRecord());
               }
         
               String mrc = sb.toString(); 
               //System.out.println("mrc: "+mrc);
               if (StringUtils.isNotEmpty(mrc)) {
                  saveBibMrc(mrc, bibid, destDir);
               }
            } catch (Exception e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
            }
      	 }
      }
      System.out.println("Done.");
      
   } 
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveBibMrc(String mrc, String bibid, String destDir) throws Exception {
      Calendar now = Calendar.getInstance();
      long ts = now.getTimeInMillis();
      String url = destDir + "/bib." + bibid +"."+ ts +".mrc";
      System.out.println("Saving mrc to: "+ url);
      try {         
         
         //FileUtils.writeStringToFile(new File("/tmp/test.mrc"), xml, "UTF-8");
         InputStream isr = IOUtils.toInputStream(mrc, StandardCharsets.UTF_8); 
         getDavService().saveFile(url, isr);

      } catch (Exception ex) {
         throw ex;
      }  
   } 
   
   /**
    * @param str
    * @return
    */
   protected InputStream stringToInputStream(String str) {
      byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
      return new ByteArrayInputStream(bytes);	
   }
   
   
    
}
