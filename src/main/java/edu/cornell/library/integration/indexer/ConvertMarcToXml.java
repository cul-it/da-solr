package edu.cornell.library.integration.indexer;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marc.MarcRecord;

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
	   
   private static void convertDir( String srcDir, String destDir) throws IOException {

      // get list of daily mrc files
      List<String> srcList = new ArrayList<>();
      //System.out.println("Getting list of marc files");
      srcList = davService.getFileList(srcDir);

      // iterate over mrc files
      if (srcList.size() == 0) {
         System.out.println("No Marc files available to process");
      } else {
         for (String srcFile  : srcList) {
            System.out.println("Converting file: "+ srcFile);
   			try {
   				convertMrcToXml(davService, srcDir, srcFile, destDir);
   			} catch (Exception e) {
   				System.out.println("Exception thrown. Could not convert file: "+ srcFile);
   				e.printStackTrace();
   			}
   		}
      }

   }
   /**
    * @param mrc
    * @param davService
    * @return
    * @throws Exception 
    */
   private static void convertMrcToXml(
		   DavService davService, String srcDir, String srcFile, String destDir) throws Exception {

	   String fileNameRoot = srcFile.replaceAll(".mrc$", "");
	   String xmlFilename = fileNameRoot + ".xml";

	   String tmpFilePath = System.getProperty("java.io.tmpdir") + "/"+ srcFile;
	   File f = davService.getFile(srcDir +"/"+ srcFile, tmpFilePath);

	   try ( FileInputStream is = new FileInputStream(f);
			   FileOutputStream out = new FileOutputStream(System.getProperty("java.io.tmpdir") + xmlFilename) ){

		   MarcRecord.marcToXml(is, out);
		   moveXmlToDav(davService, destDir, xmlFilename);
      }

      FileUtils.deleteQuietly(f);

   }

   private static void moveXmlToDav(DavService davService, String destDir, String xmlFilename) throws IOException {
       File srcFile = new File(System.getProperty("java.io.tmpdir") +"/"+ xmlFilename);
       String destFile = destDir +"/"+ xmlFilename;

       try ( InputStream isr = new FileInputStream(srcFile) ) {
          davService.saveFile(destFile, isr);
          System.out.println("Saved to webdav: "+ destFile );
          FileUtils.deleteQuietly(srcFile);
       } catch (IOException ex) {
          throw new IOException("Could not save from temp file " + srcFile + " to WEBDAV " + destFile, ex);
       }
    }

}
