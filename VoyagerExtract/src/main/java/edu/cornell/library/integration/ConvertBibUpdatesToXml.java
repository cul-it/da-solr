package edu.cornell.library.integration;



import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;


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
      converter.setSplitSize(0);
      converter.setDestDir(destDir);
      converter.setTmpDir(config.getTmpDir());
      
      // iterate over mrc files
      if (srcList.size() == 0) {
         System.out.println("No update Marc files available to "
                 + "process in '" + srcDir + "'");
         return;
      }
      
      int totalRecordCount = 0;
      for (String srcFile  : srcList) {
        System.out.println("Converting mrc file: "+ srcFile);
		try {
		   totalRecordCount += converter.convertMrcToXml(getDavService(), srcDir, srcFile).size();
		} catch (Exception e) { 
			System.out.println("Exception caught: could not "
					+ "convert file: "+ srcFile + "\n" 
					+ "due to: " );
			e.printStackTrace();
		}
     }
     System.out.println("\nTotal record count: "+totalRecordCount);

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
