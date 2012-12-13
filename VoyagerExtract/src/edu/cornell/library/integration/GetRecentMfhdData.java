package edu.cornell.library.integration;

 
import java.io.CharArrayWriter; 
import java.io.InputStream;
import java.sql.Clob;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.io.Reader; 

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import oracle.sql.*;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.LocationInfo;
import edu.cornell.library.integration.config.IntegrationDataProperties;
import edu.cornell.library.integration.service.CatalogService;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.util.ObjectUtils; 

public class GetRecentMfhdData {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService;
   private IntegrationDataProperties integrationDataProperties;
   

   /**
    * default constructor
    */
   public GetRecentMfhdData() { 
       
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
    * @return the integrationDataProperties
    */
   public IntegrationDataProperties getIntegrationDataProperties() {
      return this.integrationDataProperties;
   }


   /**
    * @param integrationDataProperties the integrationDataProperties to set
    */
   public void setIntegrationDataProperties(
         IntegrationDataProperties integrationDataProperties) {
      this.integrationDataProperties = integrationDataProperties;
   } 
   
   /**
    * @param args
    */
   public static void main(String[] args) {
     GetRecentMfhdData app = new GetRecentMfhdData();
     if (args.length != 1 ) {
        System.err.println("You must provide a destination dir as an argument");
        System.exit(-1);
     }
     String destDir = args[0];
     app.run(destDir);
   }

   /**
    * 
    */
   public void run(String destDir) {
      System.out.println("Get Recent MfhdData");
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());

       
      List<String> mfhdIdList = new ArrayList<String>();
      try {
         System.out.println("Getting recent mfhdids");
         mfhdIdList = getCatalogService().getRecentMfhdIds(getDateString());
         System.out.println("Found "+ mfhdIdList.size() +" mfhdids.");
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }

      for (String mfhdid: mfhdIdList) {
         try {            
            System.out.println("Getting mfhdRecord for mfhdid: "+mfhdid);
            MfhdBlob mfhdBlob = catalogService.getMfhdBlob(mfhdid);            
            saveMfhdData(mfhdBlob, destDir);
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
   public void saveMfhdData(MfhdBlob mfhdBlob, String destDir) throws Exception {
      try {
         String mfhdid = mfhdBlob.getMfhdId();
         String url = destDir + "/" + mfhdid +".mrc";
         
         Clob clob = mfhdBlob.getClob();
          
         char[] chars = new char[(int) clob.length()];         
         Reader reader = clob.getCharacterStream();
         
         reader.read(chars);
         CharArrayWriter caw = new CharArrayWriter();
         caw.write(chars, 0, (int) clob.length());
         String str = caw.toString();
         //FileOutputStream ostream = new FileOutputStream(new File("/tmp/test.mrc"));
         //ostream.write(str.getBytes()); 
         //ostream.close();
         
         //FileUtils.writeStringToFile(new File("/tmp/test.mrc"), str, "UTF-8");
         InputStream isr = IOUtils.toInputStream(str, "UTF-8");       
         
         getDavService().saveFile(url, isr);
      
      } catch (Exception ex) {
         throw ex;
      }
   }
   
   protected String getDateString() {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Calendar now = Calendar.getInstance();
      Calendar earlier = now;
      earlier.add(Calendar.HOUR, -3);
      String ds = df.format(earlier.getTime());
      return ds;
   }
}
