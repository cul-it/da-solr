package edu.cornell.library.integration.support;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory; 
import org.marc4j.marc.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import edu.cornell.library.integration.bo.BibData; 
import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.dao.CatalogDao;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService; 
import edu.cornell.library.integration.util.ConvertUtils;

public class ShowBibMrc {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 
 
   private CatalogDao catalogDao; 
   

   /**
    * default constructor
    */
   public ShowBibMrc() { 
       
   } 

   /**
    * @return the catalogDao
    */
   public CatalogDao getCatalogDao() {
      return this.catalogDao;
   }

   /**
    * @param catalogService the catalogService to set
    */
   public void setCatalogDao(CatalogDao catalogDao) {
      this.catalogDao = catalogDao;
   }

   
   
   
   /**
    * @param args
    */
   public static void main(String[] args) {
     ShowBibMrc app = new ShowBibMrc();
     if (args.length != 1 ) {
        System.err.println("You must provide a bibid as an argument");
        System.exit(-1);
     }
     String bibid  = args[0]; 
     app.run(bibid);
   }
   

   /**
    * 
    */
   public void run(String bibid) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogDao")) {
         setCatalogDao((CatalogDao) ctx.getBean("catalogDao"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      } 
      
      try {            
         System.out.println("Getting bib mrc for bibid: "+bibid);
         List<BibData>  bibDataList = this.catalogDao.getBibData(bibid);
         StringBuffer sb = new StringBuffer();
         for (BibData bibData : bibDataList) {
            sb.append(bibData.getRecord());
         }
         ConvertUtils convert = new ConvertUtils();
         System.out.println(sb.toString());
         Record record = convert.getMarcRecord(sb.toString());
         if (record != null) {
            System.out.println(record.toString()); 
         } else {
        	System.out.println("Record is empty");
         }
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
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
