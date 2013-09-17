package edu.cornell.library.integration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.bo.AuthData;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class GetAuthUpdatesMrc {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public GetAuthUpdatesMrc() { 
       
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
     GetAuthUpdatesMrc app = new GetAuthUpdatesMrc();
     if (args.length != 1 ) {
        System.err.println("You must provide a destination Dir as an argument");
        System.exit(-1);
     }
      
     String destDir  = args[0];
     app.run(destDir);
   }
   

   /**
    * 
    */
   public void run(String destDir) {
      
      ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");
     
      if (ctx.containsBean("catalogService")) {
         setCatalogService((CatalogService) ctx.getBean("catalogService"));
      } else {
         System.err.println("Could not get catalogService");
         System.exit(-1);
      }

      setDavService(DavServiceFactory.getDavService());
      
      // get list of authids updates using recent date String
      List<String> authIdList = new ArrayList<String>();
      try {
         System.out.println("Getting recent authids");
         authIdList = getCatalogService().getRecentAuthIds(getDateString());
      } catch (Exception e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
      
      // iterate over authids, concatenate auth data to create mrc
      for (String authid : authIdList) {
			try {
				System.out.println("Getting auth mrc for authid: " + authid);
				List<AuthData> authDataList = catalogService.getAuthData(authid);
				StringBuffer sb = new StringBuffer();
				for (AuthData authData : authDataList) {
					sb.append(authData.getRecord());
				}

				String mrc = sb.toString();
				//System.out.println("mrc: " + mrc); 
				saveAuthMrc(mrc, authid, destDir);
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
   public void saveAuthMrc(String mrc, String authid, String destDir) throws Exception {
      Calendar now = Calendar.getInstance();
      long ts = now.getTimeInMillis();
      String url = destDir + "/auth." + authid +"."+ ts +".mrc";
      //System.out.println("Saving mrc to: "+ url);
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
   
   /**
    * @return
    */
   protected String getDateString() {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	   Calendar now = Calendar.getInstance();
	   Calendar earlier = now;
	   earlier.add(Calendar.HOUR, -24);
	   String ds = df.format(earlier.getTime());
	   return ds;
   }
   
   
    
}
