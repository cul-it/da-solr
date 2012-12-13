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
import edu.cornell.library.integration.util.ObjectUtils; 

public class ConvertBib {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertBib() { 
       
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
     ConvertBib app = new ConvertBib();
     if (args.length != 2 ) {
        System.err.println("You must provide a bibid and  destination Dir as arguments");
        System.exit(-1);
     }
     String bibid  = args[0];
     String destDir  = args[1];
     app.run(bibid, destDir);
   }
   

   /**
    * 
    */
   public void run(String bibid, String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      
      try {            
         System.out.println("Getting bibRecord for bibid: "+bibid);
         List<BibData>  bibDataList = catalogService.getBibData(bibid);
         StringBuffer sb = new StringBuffer();
         for (BibData bibData : bibDataList) {
            sb.append(bibData.getRecord());
         }
         
         String mrc = sb.toString(); 
         System.out.println("mrc: "+mrc);
         String xml = convertBibData(mrc);
         System.out.println("xml: "+ xml);
         saveBibData(xml, bibid, destDir); 
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      } 
      
   }
   
   public String convertBibData(String bibStr) throws Exception {
      String xml = new String();
      char chr = bibStr.charAt(bibStr.length() - 1);
      System.out.println("terminator: "+ (int) chr); 
      String reclenstr = bibStr.substring(0,5); 
      System.out.println("reclen: "+ reclenstr);
      System.out.println("bibStrlen: "+ bibStr.length());
 
      Writer writer = null;
      InputStream is = stringToInputStream(bibStr);
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
          
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } finally {
         ostream.close();
      } 
      return xml;

   }

   public void convertBibBlob(BibBlob bibBlob, String destDir) throws Exception {
      String xml = null;
      CLOB clob =(CLOB) bibBlob.getClob();
      
      Record record = null;
      Writer writer = null;
      InputStream is = null;
      OutputStream ostream = null;
      
      try {
          
         String bibStr = convertClobToString(clob);
         
         is = stringToInputStream(bibStr);
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
         saveBibData(xml, bibBlob.getBibId(), destDir);
          
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
         throw e;
      } 
   }
      
   
   /**
    * @param xml
    * @throws Exception
    */
   public void saveBibData(String xml, String bibid, String destDir) throws Exception {
      try {         
         
         FileUtils.writeStringToFile(new File("/tmp/test.mrc"), xml, "UTF-8");
         InputStream isr = IOUtils.toInputStream(xml, "UTF-8");       
         
         String url = destDir + "/" + bibid +".mrc";      
         getDavService().saveFile(url, isr);
      
      } catch (UnsupportedEncodingException ex) {
         throw ex;
      } catch (Exception ex) {
         throw ex;
      }  
   }
   
   protected static String convertClobToString(CLOB clob)  { 
      
      try {        
         System.out.println("clob length: "+ clob.length()); 
         char clobVal[] = new char[(int) clob.length()];
         char[]  reclen = new char[(int) 5];
         clob.getChars(0L, (int) 5, reclen);
          
         Reader r = clob.getCharacterStream();
         r.read(clobVal);         
         r.close();
         String record_length = new String(clobVal, 0, 5); 
         System.out.println("record_length "+ record_length);
         StringWriter sw = new StringWriter();         
         sw.write(clobVal);        
         String bibStr = StringUtils.stripEnd(sw.toString(), " ");
         sw.close();
         return bibStr;
      } catch(Exception e) { 
         e.printStackTrace();
         return "";
      }   
      
   }
   
   protected InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
      byte[] bytes = str.getBytes("UTF-8");
      return new ByteArrayInputStream(bytes);	
   }
   
   
    
}
