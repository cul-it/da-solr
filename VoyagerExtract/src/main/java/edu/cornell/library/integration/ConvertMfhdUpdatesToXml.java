package edu.cornell.library.integration;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

/**
 * Get the list of files in the MFHD updates directory, 
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
 * that ConvertMfhdUpdatesToXml is a step in the voyager to Solr
 * process. 
 *
 */
public class ConvertMfhdUpdatesToXml extends VoyagerToSolrStep {

    public static void main(String[] args) throws Exception {
        
        VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration
                .loadConfig(args);

        ConvertMfhdUpdatesToXml app = new ConvertMfhdUpdatesToXml();        
        app.run( config );
    }

    public void run(VoyagerToSolrConfiguration config) throws Exception {
        setDavService(DavServiceFactory.getDavService(config));                
        
        String srcDir = config.getWebdavBaseUrl() + "/" + config.getDailyMfhdDir();
        String destDir = config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir();
               
        String badDir = srcDir + ".bad";
        String doneDir = srcDir + ".done";
        
        getDavService().mkDir( badDir );
        getDavService().mkDir( doneDir );
        
        // get list of mfhdids updates using recent date String
        List<String> srcList = new ArrayList<String>();
        try {
            System.out.println("Getting list of MFHD MARC files from "
                    + "'" + config.getDailyMfhdDir() +"'") ;
            srcList = getDavService().getFileList(srcDir);
        } catch (Exception e) {
            throw new Exception("could not get a list of MFHD MARC "
                    + "files from " + srcDir, e);
        }
        
        MrcToXmlConverter converter = new MrcToXmlConverter();
        converter.setSrcType("mfhd");
        converter.setExtractType("updates");
        converter.setSplitSize(10000);
        converter.setDestDir( destDir );
        converter.setTmpDir( config.getTmpDir() );
                
        if (srcList.size() == 0) {
            System.out.println("No update Marc files available to process in "
                    + config.getDailyMfhdDir());
            return;
        }
        
        // iterate over mrc files
        for (String srcFile : srcList) {
            try {                    
                converter.setTs( getTimestampFromFileName(srcFile) );                    
                converter.setItemId(getSeqnoFromFileName(srcFile));
                converter.convertMrcToXml(getDavService(), srcDir, srcFile);

                getDavService().moveFile(srcDir  + "/" + srcFile, 
                                         doneDir + "/" + srcFile);
            } catch (Exception e) {
                try {
                    System.out.println("Exception caught: could not "
                            + "convert file: " + srcFile + "\n" 
                            + "due to " + e.getMessage() 
                            + "moving source file to bad dir at '" + badDir + "'" );
                    getDavService().moveFile(srcDir + "/" + srcFile,
                            badDir + "/" + srcFile);
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
    private  String getTimestampFromFileName(String srcFile) {
        String[] tokens = StringUtils.split(srcFile, ".");
        return tokens[2];

    }

    /**
     * @param srcFile
     * @return
     */
    private String getSeqnoFromFileName(String srcFile) {
        String[] tokens = StringUtils.split(srcFile, ".");
        return tokens[3];
    }
    

}
