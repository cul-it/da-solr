package edu.cornell.library.integration;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream; 
import java.io.InputStream;
import java.io.OutputStream; 
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException; 
import java.io.Writer;
import java.util.List;

 
import javax.xml.transform.Result;
import javax.xml.transform.Source; 
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
 
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.ErrorHandler;
import org.marc4j.MarcReader;
import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcResult;
import org.marc4j.marcxml.MarcSource;
import org.marc4j.marcxml.MarcXmlReader; 
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.w3c.dom.Document; 
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer; 
import org.xml.sax.InputSource;
 
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.util.ObjectUtils; 

public class Marc2MarcXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   private String srcDir;
   private String destDir;   

   /**
    * default constructor
    */
   public Marc2MarcXml() { 
       
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
    * @return the srcDir
    */
   public String getSrcDir() {
      return this.srcDir;
   }

   /**
    * @param srcDir the srcDir to set
    */
   public void setSrcDir(String srcDir) {
      this.srcDir = srcDir;
   }

   /**
    * @return the destDir
    */
   public String getDestDir() {
      return this.destDir;
   }

   /**
    * @param destDir the destDir to set
    */
   public void setDestDir(String destDir) {
      this.destDir = destDir;
   } 
   
   /**
    * @param args
    */
   public static void main(String[] args) {
     Marc2MarcXml app = new Marc2MarcXml();
     if (args.length != 2 ) {
        System.err.println("You must provide a src and destination Dir as arguments");
        System.exit(-1);
     }
     app.setSrcDir(args[0]);
     app.setDestDir(args[1]);
     app.run();
   }
   

   /**
    * 
    */
   public void run() {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");    
      setDavService(DavServiceFactory.getDavService());
      
      try {            
         System.out.println("Getting src files...");
         List<String> srcFiles = getDavService().getFileList(this.getSrcDir());
         for (String srcFile: srcFiles) {
            System.out.println("converting srcFile: "+ this.getSrcDir() + "/" + srcFile); 
            String xml = convert(this.getSrcDir()+ "/" + srcFile);
            //System.out.println(StringUtils.substring(xml, 0, 100));
            saveAsXml(xml, srcFile, this.getDestDir());
         }
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public String convert(String srcFile) throws Exception {
      String xml = new String();        
      TaggedWriter handler = new TaggedWriter();
      ErrorHandler errorHandler = new MarcErrorHandler();
      Writer writer = null;      
      InputStream is = getDavService().getFileAsInputStream(srcFile);
      
      OutputStream ostream = null;
      try {
         
         
         ostream = new ByteArrayOutputStream();
         
         MarcXmlReader producer = new MarcXmlReader(); 
         InputSource in = new InputSource(is);
         in.setEncoding("UTF-8");
         Source source = new SAXSource(producer, in);
         OutputStreamWriter osw = new OutputStreamWriter(ostream, "UTF-8");         
         writer = new BufferedWriter(osw);
          
         Result result = new StreamResult(writer);
         MarcResult marcResult = new MarcResult(handler);
         
         Converter converter = new Converter();
         converter.convert(source, marcResult);
         xml = new String(ostream.toString());
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } finally {
         ostream.close();
         writer.close();
      } 
      return xml;

   }

   
      
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveAsXml(String xml, String fname, String destDir) throws Exception {
      try {         
         String ofile = StringUtils.replace(fname, ".mrc", ".xml");
          
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8");       
         
         String url = destDir + "/" + ofile;      
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   }
   
   
   
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   public String serialize(Document doc)    {
      DOMImplementationLS domImplementation = (DOMImplementationLS) doc.getImplementation();
      LSSerializer lsSerializer = domImplementation.createLSSerializer();
      return lsSerializer.writeToString(doc);   
  }
   
   
    
}
