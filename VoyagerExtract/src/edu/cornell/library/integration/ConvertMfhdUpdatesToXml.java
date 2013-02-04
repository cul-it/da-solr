package edu.cornell.library.integration;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter; 
import java.io.UnsupportedEncodingException;
import java.io.Writer;
 
import java.text.SimpleDateFormat;
 
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
 
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.xml.sax.InputSource;
 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils;
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertMfhdUpdatesToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdUpdatesToXml() { 
       
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
     ConvertMfhdUpdatesToXml app = new ConvertMfhdUpdatesToXml();
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
      ConvertUtils converter = new ConvertUtils();
      converter.setSrcType("mfhd");
      converter.setExtractType("updates");
      converter.setSplitSize(0);
      converter.setDestDir(destDir);
      String destXmlFile = new String();
      // iterate over mrc files
      for (String srcFile  : srcList) {
         //System.out.println("Converting mfhd mrc file: "+ srcFile);
			try {
			   String mfhdid = getMfhdIdFromFileName(srcFile);			   
			   converter.setSequence_prefix(Integer.parseInt(mfhdid));
			   String ts = getTimestampFromFileName(srcFile);
			   converter.setTs(ts);
			   String mrc = davService.getFileAsString(srcDir + "/" +srcFile);
				String xml = converter.convertMrcToXml(mrc, davService);
				destXmlFile = StringUtils.replace(srcFile, ".mrc", ".xml");
				saveMfhdXml(xml, destDir, destXmlFile);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
      
   }
   
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveMfhdXml(String xml, String destDir, String destXmlFile) throws Exception {
      String url = destDir + "/" + destXmlFile;
      //System.out.println("Saving xml to: "+ url);
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
   
   public String getTimestampFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      return tokens[2];
   }
   
   public String getMfhdIdFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      return tokens[1];
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
