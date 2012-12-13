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
import oracle.sql.*;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

 
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcXmlReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;
import org.xml.sax.InputSource;
 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory; 
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertBibFull {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;    
   

   /**
    * default constructor
    */
   public ConvertBibFull() { 
       
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
    * @param args
    */
   public static void main(String[] args) {
     ConvertBibFull app = new ConvertBibFull();
     if (args.length != 2 ) {
        System.err.println("You must provide a src and  destination Dir as arguments");
        System.exit(-1);
     }
     String srcDir = args[0];
     String destDir  = args[1];
     app.run(srcDir, destDir);
   }
   

   /**
    * 
    */
   public void run(String srcDir, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
      setDavService(DavServiceFactory.getDavService());
      
      try {            
         System.out.println("Converting Full Bib records" );
         List<String> fileList = getDavService().getFileList(srcDir);
         for (String filename: fileList) {
            //System.out.println("file: "+ s);
            convertBibData(filename, srcDir, destDir);
         }
         
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public void convertBibData(String filename, String srcDir, String destDir) throws Exception {
      String xml;
       
      Writer writer = null;
      System.out.println("srcFile: "+ srcDir +"/"+ filename);
      InputStream is = getDavService().getFileAsInputStream(srcDir +"/"+ filename);
      OutputStream ostream = null;
      
      
      try {
         
         
         ostream = new ByteArrayOutputStream();
         
         MarcXmlReader producer = new MarcXmlReader();
         org.marc4j.MarcReader reader = new org.marc4j.MarcReader(); 
         InputSource in = new InputSource(is);
         in.setEncoding("UTF-8");
         Source source = new SAXSource(producer, in);
          
         writer = new BufferedWriter(new OutputStreamWriter(ostream, "UTF-8"));
         Result result = new StreamResult(writer);
         Converter converter = new Converter();
         converter.convert(source, result);
         xml = new String(ostream.toString());
         ostream.close();
         
         //System.out.println("xml: "+ xml);
         String destFile = StringUtils.replace(filename, ".mrc", ".xml");
         System.out.println("destFile: "+ destDir +"/"+ destFile);
         InputStream xmlInputStream = IOUtils.toInputStream(xml);
         getDavService().saveFile(destDir +"/"+ destFile, xmlInputStream);
         xmlInputStream.close();
          
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } 
   }
      
   
    
}
