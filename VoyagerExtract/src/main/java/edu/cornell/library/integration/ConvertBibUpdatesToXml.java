package edu.cornell.library.integration;



import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.service.CatalogService;


/**
 * Get the list of files in the BIB updates directory, 
 * For each file do:
 *   1) Convert the file to XML
 *   2) Move the source MARC file to the "done" directory or
 *      if there is an Exception, move the XML to the "bad" directory.
 *  
 * This class has a main that uses the standard VoyagerToSolrConfiguration
 * command line parameters and properties. 
 *  
 * This class extends VoyagerToSolrStep but it doesn't use 
 * much from that class. It is primarily extended to indicate
 * that this is a step in the voyager to Solr process. 
 *
 */
public class ConvertBibUpdatesToXml extends VoyagerToSolrStep{
     
   public static void main(String[] args) throws Exception {        
     VoyagerToSolrConfiguration config  = VoyagerToSolrConfiguration.loadConfig(args);
     new ConvertBibUpdatesToXml().run(config);     
   }
         
   public void run( VoyagerToSolrConfiguration config ) throws Exception{
                 
      setDavService( DavServiceFactory.getDavService(config) );
      
      String srcDir = config.getWebdavBaseUrl() + "/" + config.getDailyMrcDir();
      String destDir = config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir();
      
      String badDir = srcDir +".bad";
      String doneDir = srcDir +".done"; 
      
      getDavService().mkDir( badDir );
      getDavService().mkDir( doneDir );
      
      // get list of bibids updates using recent date String
      List<String> srcList = new ArrayList<String>();
      try {
         System.out.println("Getting list of bib marc files from '" 
                 + srcDir + "'");
         srcList = getDavService().getFileList(srcDir);
      } catch (Exception e) {
          throw new Exception("Could not get list of BIB MARC files for "
                  + "update from: '" + srcDir + "'", e);
      }
      
      MrcToXmlConverter converter = new MrcToXmlConverter();
      converter.setSrcType("bib");
      converter.setExtractType("updates");
      converter.setSplitSize(10000);
      converter.setDestDir(destDir);
      converter.setTmpDir(config.getTmpDir());
      
      // iterate over mrc files
      if (srcList.size() == 0) {
         System.out.println("No update Marc files available to "
                 + "process in '" + srcDir + "'");
         return;
      }
      
     for (String srcFile  : srcList) {
        System.out.println("Converting mrc file: "+ srcFile);
		try {
		   String ts = getTimestampFromFileName(srcFile);
           converter.setTs(ts);
           String seqno = getSeqnoFromFileName(srcFile);
           converter.setItemId(seqno); 
		   converter.convertMrcToXml(getDavService(), srcDir, srcFile);
		   getDavService().moveFile(srcDir +"/" +srcFile, doneDir +"/"+ srcFile);
		} catch (Exception e) { 
		   try {
		      System.out.println("Exception caught: could not "
		              + "convert file: "+ srcFile + "\n" 
		              + "due to " + e.getMessage() + "\n" 
		              + "moving source file to bad dir at '" + badDir + "'" );		      
		      getDavService().moveFile(srcDir +"/" +srcFile, badDir +"/"+ srcFile);
           } catch (Exception e1) {
               System.out.println("Error while trying to handle bad file,"
                       + " could not move to bad dir: " + srcFile + "\n"
                       + "due to " + e1.getMessage());
               e1.printStackTrace();
           }
		}
	}            
   } 
       
   
   /**
    * @param srcFile
    * @return
    */
   public String getTimestampFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      return tokens[2];       
   }
   
   /**
    * @param srcFile
    * @return
    */
   public String getSeqnoFromFileName(String srcFile) {
      String[] tokens = StringUtils.split(srcFile, ".");
      return tokens[3];
   }
    
}
