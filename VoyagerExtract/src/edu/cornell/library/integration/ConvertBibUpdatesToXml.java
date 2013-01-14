package edu.cornell.library.integration;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Clob;
import java.text.SimpleDateFormat;

import oracle.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

 
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marc.Record;
import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcXmlReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.xml.sax.InputSource;
 
import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData; 
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils;
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertBibUpdatesToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertBibUpdatesToXml() { 
       
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
     ConvertBibUpdatesToXml app = new ConvertBibUpdatesToXml();
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
       
      // get list of bibids updates using recent date String
      List<String> srcList = new ArrayList<String>();
      try {
         System.out.println("Getting list of bib marc files");
         srcList = davService.getFileList(srcDir);
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      // iterate over mrc files
      for (String srcFile  : srcList) {
         System.out.println("Converting mrc file: "+ srcFile);
			try {
			   String bibid = StringUtils.replace(srcFile, ".mrc", "");
			   String mrc = davService.getFileAsString(srcDir + "/" +srcFile); 
				//System.out.println("mrc: " + mrc);
				String xml = ConvertUtils.convertMrcToXml(mrc);
				if (StringUtils.isEmpty(xml)) {
				   System.out.println("Could not convert file: "+ srcFile);
               davService.moveFile(srcDir +"/" +srcFile, badDir +"/"+ srcFile);
				} else { 
				   saveBibXml(xml, bibid, destDir);
				}
			} catch (Exception e) { 
			   try {
			      System.out.println("Could not convert file: "+ srcFile);
			      //e.printStackTrace();
               davService.moveFile(srcDir +"/" +srcFile, badDir +"/"+ srcFile);
            } catch (Exception e1) {
               // TODO Auto-generated catch block
               e1.printStackTrace();
            } 
			}
		}
      
   }
   
    
   /**
    * @param xml
    * @throws Exception
    */
   public void saveBibXml(String xml, String bibid, String destDir) throws Exception {
      String url = destDir + "/" + bibid +".xml";
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
