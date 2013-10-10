package edu.cornell.library.integration.ilcommons.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;

public class FileNameUtils {

    

    /**
     * Utility method for finding most recent files in a directory 
     * where the file names follow the pattern :
     *  
     *  directoryURL/fileNamePrefix-yyyy-MM-dd.txt
     *  
     *  Such as:
     *  
     *  http://example.com/files/bibs-2013-12-27.txt
     *  http://example.com/files/bibs-2013-12-28.txt
     *  http://example.com/files/bibs-2013-12-29.txt
     *  
     * @param davService 
     * @param directoryURL - directory to check for most recent files
     * @param fileNamePrefix - prefix for file names
     * @return name of most recent file, or null if none found. 
     *   The returned string will be the full URL to the file.
     * @throws Exception if there is a problem with the WEBDAV service. 
     */
    public static String findMostRecentFile(DavService davService , String directoryURL, String fileNamePrefix ) throws Exception{
       return findMostRecentFile(davService,directoryURL,fileNamePrefix,".txt");
    }

    
    /**
     * Utility method for finding most recent files in a directory 
     * where the file names follow the pattern :
     *  
     *  directoryURL/fileNamePrefix-yyyy-MM-dd.fileNamePostfix
     *  
     *  Such as:
     *  
     *  http://example.com/files/bibs-2013-12-27.nt.gz
     *  http://example.com/files/bibs-2013-12-28.nt.gz
     *  http://example.com/files/bibs-2013-12-29.nt.gz
     *  
     * @param davService 
     * @param directoryURL - directory to check for most recent files
     * @param fileNamePrefix - prefix for file names
     * @param fileNamePostfix - ending for file names
     * 
     * @return name of most recent file, or null if none found. 
     *   The returned string will be the full URL to the file.
     * @throws Exception if there is a problem with the WEBDAV service. 
     */
    public static String findMostRecentFile(
            DavService davService, 
            String directoryURL, 
            String fileNamePrefix, 
            String fileNamePostfix)throws Exception{
        
        if( ! directoryURL.endsWith("/"))
            directoryURL = directoryURL + "/";
        
        if( ! fileNamePostfix.startsWith("."))
            fileNamePostfix = "." + fileNamePostfix;
        
        Pattern p = Pattern.compile(fileNamePrefix + "-(....-..-..)" + fileNamePrefix);
        Date lastDate = new SimpleDateFormat("yyyy").parse("1900");
        String mostRecentFile = null;
                               
        List<String> biblists = davService.getFileList( directoryURL );   
        
        Iterator<String> i = biblists.iterator();            
        while (i.hasNext()) {
            String fileName = i.next();
            Matcher m = p.matcher(fileName);
            if (m.matches()) {
                Date thisDate = new SimpleDateFormat("yyyy-MM-dd").parse(m.group(1));
                if (thisDate.after(lastDate)) {
                    lastDate = thisDate;
                    mostRecentFile = fileName;
                }
            }
        }
        return directoryURL +  mostRecentFile;
        
    }
    
    /**
     * Find the most recent unsuppressed BIB ID update file.
     */
    public static String findMostRecentUnsuppressedBibIdFile(
            VoyagerToSolrConfiguration config, 
            DavService davService) throws Exception {
        try {
            return FileNameUtils.findMostRecentFile(davService, 
                    config.getWebdavBaseUrl()
                    + config.getDailyBibUnsuppressedDir(),
                    config.getDailyBibUnsuppressedFilenamePrefix());
        } catch (Exception cause) {
            throw new Exception("Could not find most recent bib file", cause);
        }
    }

    /**
     * Find the most recent unsuppressed MFHD ID update file.
     */
    public static String findMostRecentUnsuppressedMfhdIdFile(
            VoyagerToSolrConfiguration config, 
            DavService davService) throws Exception {
        try {
            return FileNameUtils.findMostRecentFile(davService, 
                    config.getWebdavBaseUrl()
                    + config.getDailyMfhdUnsuppressedDir(),
                    config.getDailyMfhdUnsuppressedFilenamePrefix());
        } catch (Exception e) {
            throw new Exception("Could not get most recent Mfhd holding file.",
                    e);
        }
    }
}
