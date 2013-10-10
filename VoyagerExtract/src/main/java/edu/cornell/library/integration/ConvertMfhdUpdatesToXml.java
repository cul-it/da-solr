package edu.cornell.library.integration;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.util.ConvertUtils;

/**
 * Get the list of files in the MFHD updates directory, 
 * For each file do:
 *   1) Convert the file to XML
 *   2) Move the source MARC file to the "done" directory or
 *      if there is an Exception, move the XML to the "bad" directory.
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

    private void run(VoyagerToSolrConfiguration config) throws Exception {
        setDavService(DavServiceFactory.getDavService(config));                
        
        String srcDir = config.getDailyMfhdDir();        
        String badDir = srcDir + ".bad";
        String doneDir = srcDir + ".done";
        
        // get list of mfhdids updates using recent date String
        List<String> srcList = new ArrayList<String>();
        try {
            System.out.println("Getting list of mfhd marc files");
            srcList = getDavService().getFileList(srcDir);
        } catch (Exception e) {
            throw new Exception("could not get a lit of MFHD MARC "
                    + "files from " + srcDir, e);
        }
        
        ConvertUtils converter = new ConvertUtils();
        converter.setSrcType("mfhd");
        converter.setExtractType("updates");
        converter.setSplitSize(10000);
        converter.setDestDir( config.getDailyBibMrcXmlDir() );
        
        // iterate over mrc files
        if (srcList.size() == 0) {
            System.out.println("No update Marc files available to process");
        } else {
            for (String srcFile : srcList) {
                try {
                    String ts = getTimestampFromFileName(srcFile);
                    converter.setTs(ts);
                    String seqno = getSeqnoFromFileName(srcFile);
                    converter.setItemId(seqno);
                    converter.convertMrcToXml(getDavService(), srcDir, srcFile);
                    getDavService().moveFile(srcDir + "/" + srcFile, doneDir + "/"
                            + srcFile);
                } catch (Exception e) {
                    try {
                        System.out.println("Exception caught: could not "
                                + "convert file: " + srcFile + "\n" 
                                + "due to " + e.getMessage() );                        
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
