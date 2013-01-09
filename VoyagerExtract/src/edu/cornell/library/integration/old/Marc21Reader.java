package edu.cornell.library.integration.old;

 
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream; 
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

 
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marc.Record;
import org.marc4j.marcxml.Converter;
import org.marc4j.marcxml.MarcXmlReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.xml.sax.InputSource;

 
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.util.ObjectUtils;

public class Marc21Reader {
   
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass());
   
   private DavService davService;

   public Marc21Reader() { 
       
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
   
   public void run() {
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");  
      setDavService(DavServiceFactory.getDavService());
      
      String url = "http://jaf30-dev.library.cornell.edu/data/voyager/bib/bib.mrc.updates/5430043.mrc";
      String fname = "/tmp/test.mrc";
      Writer writer = null;
      OutputStream ostream = null;
      
      try {
         Record record = null;
         //InputStream is = getDavService().getFileAsInputStream(url);
         FileInputStream is = new FileInputStream(new File(fname));
         MarcXmlReader producer = new MarcXmlReader();
         org.marc4j.MarcReader reader = new org.marc4j.MarcReader(); 
         InputSource in = new InputSource(is);
         in.setEncoding("UTF-8");
         Source source = new SAXSource(producer, in);
          
         writer = new BufferedWriter(new OutputStreamWriter(ostream, "UTF-8"));
         Result result = new StreamResult(writer);
         Converter converter = new Converter();
         converter.convert(source, result);
         String xml = new String(ostream.toString());
          
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      
   }
   
   
   
   /**
    * @param args
    */
   public static void main(String[] args) {       
      Marc21Reader marcReader = new Marc21Reader();     
      marcReader.run();      
   }
   
   

}
