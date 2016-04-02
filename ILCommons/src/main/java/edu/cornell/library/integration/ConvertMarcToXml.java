package edu.cornell.library.integration;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class ConvertMarcToXml {
   
   /** Logger for this class and subclasses */
   protected final Log logger = LogFactory.getLog(getClass()); 

   private static DavService davService;
 
   /**
    * @param args
    */
   public static void main(String[] args) {

	   Collection<String> requiredFields = SolrBuildConfig.getRequiredArgsForWebdav();
	   requiredFields.add("marc2XmlDirs"); 
	   SolrBuildConfig config  = SolrBuildConfig.loadConfig(args,requiredFields);

	   try {
		   new ConvertMarcToXml(config);
	   } catch (IOException e) {
		   e.printStackTrace();
		   System.exit(1);
	   }
   }

   /**
    * default constructor
 * @throws IOException 
    */
   public ConvertMarcToXml(SolrBuildConfig config) throws IOException { 
	   davService = DavServiceFactory.getDavService(config);

	   String[] dirs = null;
	   try {
		   dirs = config.getMarc2XmlDirs();
	   } catch (IOException e) {
		   e.printStackTrace();
		   System.exit(1);
	   }
	   if (dirs == null) return;
	   if (dirs.length % 2 != 0) { 
		   System.out.println("marc2XmlDirs must be configured with an even number of paths in src/dest pairs.");
		   return;
	   }
	   System.out.println("\nConvert MARC to MARC XML");
	   for (int i = 0; i < dirs.length; i += 2) {
		   convertDir(dirs[i], dirs[i+1]);
	   }
   }
	   
   private void convertDir( String srcDir, String destDir) throws IOException {
      
      // get list of daily mrc files
      List<String> srcList = new ArrayList<String>();
      //System.out.println("Getting list of marc files");
      srcList = davService.getFileList(srcDir);
      MrcToXmlConverter converter = new MrcToXmlConverter();
      converter.setDestDir(destDir);
      
      
      // iterate over mrc files
      int totalRecordCount = 0;
      if (srcList.size() == 0) {
         System.out.println("No Marc files available to process");
      } else {
         for (String srcFile  : srcList) {
            System.out.println("Converting file: "+ srcFile);
   			try {
   				totalRecordCount += converter.convertMrcToXml(davService, srcDir, srcFile).size();
   			} catch (Exception e) {
   				System.out.println("Exception thrown. Could not convert file: "+ srcFile);
   				e.printStackTrace();
   			}
   		}
      }
      System.out.println("Total record count for directory: "+totalRecordCount);
      
   }
}
