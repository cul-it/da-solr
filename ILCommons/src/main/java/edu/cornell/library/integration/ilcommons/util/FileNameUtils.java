package edu.cornell.library.integration.ilcommons.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
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
       List<String> files = findMostRecentFiles(davService,directoryURL,fileNamePrefix,".txt");
       if ((files == null) || files.isEmpty()) return null;
       return files.iterator().next();
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
    public static List<String> findMostRecentFiles(
            DavService davService, 
            String directoryURL, 
            String fileNamePrefix, 
            String fileNamePostfix)throws Exception{
        
        if( ! directoryURL.endsWith("/"))
            directoryURL = directoryURL + "/";
        
        if( ! fileNamePostfix.startsWith("."))
            fileNamePostfix = "." + fileNamePostfix;
        
        Pattern p = Pattern.compile(fileNamePrefix + "-(....-..-..)(.\\d+)?" + fileNamePostfix);
        Date lastDate = new SimpleDateFormat("yyyy").parse("1900");
        List<String> mostRecentFiles = new ArrayList<String>();
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
                    mostRecentFiles.clear();
                    mostRecentFiles.add(directoryURL + fileName);
                    mostRecentFile = fileName;
                } else if (thisDate.equals(lastDate)) {
                    mostRecentFiles.add(directoryURL + fileName);
                }
            }
        }
        
        if( mostRecentFile == null )
            return mostRecentFiles;
        
        return mostRecentFiles;
        
    }
    
    /**
     * Find the most recent unsuppressed BIB ID update file.
     */
    public static String findMostRecentUnsuppressedBibIdFile(
            SolrBuildConfig config, 
            DavService davService) throws Exception {
        
        String dirToLookIn = 
                config.getWebdavBaseUrl() + "/" + config.getDailyBibUnsuppressedDir();
        
        try {
            return FileNameUtils.findMostRecentFile(davService,
                    dirToLookIn,                    
                    config.getDailyBibUnsuppressedFilenamePrefix());
        } catch (Exception cause) {
            throw new Exception("Could not find most recent unsuppressed BIB file with prefix " 
                    + config.getDailyBibUnsuppressedFilenamePrefix()
                    + " in WEBDAV directory " + dirToLookIn  , cause);
        }
    }

    /**
     * Find the most recent unsuppressed MFHD ID update file.
     */
    public static String findMostRecentUnsuppressedMfhdIdFile(
            SolrBuildConfig config, 
            DavService davService) throws Exception {
        
        String dirToLookIn = 
                config.getWebdavBaseUrl() + "/" + config.getDailyMfhdUnsuppressedDir();
        
        try {
            return FileNameUtils.findMostRecentFile(davService, 
                    dirToLookIn,
                    config.getDailyMfhdUnsuppressedFilenamePrefix());
        } catch (Exception e) {
            throw new Exception("Could not get most recent unsuppressed MFHD holding file." 
                    + " with prefix " + config.getDailyMfhdUnsuppressedFilenamePrefix() 
                    + " in WEBDAV directory " + dirToLookIn , e);
        }
    }

    /**
     * Find the most recent item list file.
     */
    public static String findMostRecentItemListFile(
            SolrBuildConfig config, 
            DavService davService) throws Exception {
        
        String dirToLookIn = 
                config.getWebdavBaseUrl() + "/" + config.getDailyItemDir();
        
        try {
            return FileNameUtils.findMostRecentFile(davService, 
                    dirToLookIn,
                    config.getDailyItemFilenamePrefix());
        } catch (Exception e) {
            throw new Exception("Could not get most recent item list file." 
                    + " with prefix " + config.getDailyItemFilenamePrefix() 
                    + " in WEBDAV directory " + dirToLookIn , e);
        }
    }
}
