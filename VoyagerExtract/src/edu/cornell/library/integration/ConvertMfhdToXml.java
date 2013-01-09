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
import edu.cornell.library.integration.util.ConvertUtils;
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertMfhdToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdToXml() { 
       
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
     ConvertMfhdToXml app = new ConvertMfhdToXml();
     if (args.length != 3 ) {
        System.err.println("You must provide a mfhdid and  destination Dir as arguments");
        System.exit(-1);
     }
     String mfhdid  = args[0];
     String srcDir  = args[1];
     String destDir  = args[2];
     app.run(mfhdid, srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String mfhdid, String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      
      try {            
         System.out.println("Getting mfhd mrc for bibid: "+mfhdid);
         String mrc = davService.getFileAsString(srcDir + "/" + mfhdid + ".mrc"); 
         //System.out.println("mrc: "+mrc);
         String xml = ConvertUtils.convertMrcToXml(mrc);
         //System.out.println("xml: "+ xml);
         saveMfhdXml(xml, mfhdid, destDir); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
    
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdXml(String xml, String mfhdid, String destDir) throws Exception {
      String url = destDir + "/" + mfhdid +".xml";
      System.out.println("Saving xml to: "+ url);
      try {         
         
         //FileUtils.writeStringToFile(new File("/tmp/test.mrc"), xml, "UTF-8");
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8"); 
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   } 
   
    
    
}
