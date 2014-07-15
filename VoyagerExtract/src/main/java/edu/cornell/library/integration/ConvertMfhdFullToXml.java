package edu.cornell.library.integration;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;

public class ConvertMfhdFullToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private DavService davService;
   private CatalogService catalogService; 
   

   /**
    * default constructor
    */
   public ConvertMfhdFullToXml() { 
       
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
     ConvertMfhdFullToXml app = new ConvertMfhdFullToXml();
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

		ApplicationContext ctx = new ClassPathXmlApplicationContext(
				"spring.xml");

		if (ctx.containsBean("catalogService")) {
			setCatalogService((CatalogService) ctx.getBean("catalogService"));
		} else {
			System.err.println("Could not get catalogService");
			System.exit(-1);
		}

		setDavService(DavServiceFactory.getDavService());
		// get list of Full mrc files
		List<String> srcList = new ArrayList<String>();
		try {
			// System.out.println("Getting list of Mfhd marc files");
			srcList = davService.getFileList(srcDir);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Get File handle for saving mfhdid list
/* We might want to restore the tracking of id lists in the future, but differently.
 *		File fh = new File(
 *				"/usr/local/src/integrationlayer/VoyagerExtract/mfhds-full-"
 *						+ getTodayString() + ".txt");
 *		FileOutputStream fout = null;
 *		try {
 *			fout = new FileOutputStream(fh);
 *		} catch (FileNotFoundException e2) {
 *			// TODO Auto-generated catch block
 *			e2.printStackTrace();
 *		}
 *		List<String> mfhdlist = new ArrayList<String>();  */		
		MrcToXmlConverter converter = new MrcToXmlConverter();
		converter.setSrcType("mfhd");
		converter.setExtractType("full");
		converter.setSplitSize(10000);
		converter.setDestDir(destDir);
		// iterate over mrc files
		if (srcList.size() == 0) {
			System.out.println("No Full Marc files available to process");
		} else {
			for (String srcFile : srcList) {
				System.out.println("Converting mrc file: "+ srcFile);
				try {
					converter.convertMrcToXml(davService, srcDir, srcFile);
/*					if (mfhdlist.size() > 0 ) {
 *					   saveMfhdList(fout, mfhdlist); 
 *					} */
				} catch (Exception e) {
					try {
						System.out
								.println("Exception thrown. Could not convert file: "
										+ srcFile);
						e.printStackTrace();
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
		}
/*		
 *		if (fout != null) {
 *			try {
 *				fout.close();
 *			} catch (IOException e) {
 *				// TODO Auto-generated catch block
 *				e.printStackTrace();
 *			}
 *		} */

	}
   
    
   
   /**
    * @param str
    * @return
    * @throws UnsupportedEncodingException
    */
   protected  InputStream stringToInputStream(String str) throws UnsupportedEncodingException {
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
   
   protected String getTodayString() {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	   Calendar today = Calendar.getInstance(); 
	   String ds = df.format(today.getTime());
	   return ds;
   }
   
   protected void saveMfhdList(OutputStream out, List<String> mfhdlist) throws Exception {
	   StringBuffer sb = new StringBuffer();
	   for (String s: mfhdlist) {
		   sb.append(s +"\n");
	   }
	   out.write(sb.toString().getBytes());
   }
   
   
    
}
