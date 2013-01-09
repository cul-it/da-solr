package edu.cornell.library.integration;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream; 
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter; 
import java.io.UnsupportedEncodingException;
import java.io.Writer; 
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
 
import org.apache.commons.io.IOUtils; 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcXmlReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.xml.sax.InputSource;
 
  
import edu.cornell.library.integration.bo.MfhdData; 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetMfhdMrc {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetMfhdMrc() { 
       
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
     GetMfhdMrc app = new GetMfhdMrc();
     if (args.length != 2 ) {
        System.err.println("You must provide a mfhd id and  destination Dir as arguments");
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
      
      try {            
         System.out.println("Getting mfhdRecord for mfhdid: "+mfhdid);
         List<MfhdData>  mfhdDataList = catalogService.getMfhdData(mfhdid);
         StringBuffer sb = new StringBuffer();
         for (MfhdData mfhdData : mfhdDataList) {
            sb.append(mfhdData.getRecord());
         }
         
         String mrc = sb.toString(); 
         //System.out.println("mrc: "+mrc); 
         saveMfhdMrc(mrc, mfhdid, destDir); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
    
   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdMrc(String mrc, String mfhdid, String destDir) throws Exception {
      String url = destDir + "/" + mfhdid +".mrc";
      System.out.println("Saving mrc to: "+ url);
      try {         
         
         //FileUtils.writeStringToFile(new File("/tmp/test.mrc"), mrc, "UTF-8");
         InputStream isr = IOUtils.toInputStream(mrc, "UTF-8"); 
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
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
