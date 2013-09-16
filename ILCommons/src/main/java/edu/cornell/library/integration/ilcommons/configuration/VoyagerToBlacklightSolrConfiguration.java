package edu.cornell.library.integration.ilcommons.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is a basic structure intended to hold all the configuration information
 * needed for all steps of the Voyager MARC21 extract and convert to
 * Blacklight Solr index.
 * 
 * The goal is to facilitate the creation of dev, staging and production
 * configurations that can be called for from the command line of
 * the different steps of the conversion.
 * 
 */
public class VoyagerToBlacklightSolrConfiguration {
    /** 
     * Base part of webDav URL. The following directories will be appended to this base. 
     */
    String webdavBaseUrl;
    
    String webdavUser;
    String webdavPassword;
    
    /* ********* properties used for FULL extract ************** */
    /** 
     * Directory of Mrc21 extract. This dir will be appended to the webdavBaseURL.
     */
    String fullMrc21Dir;
    
    String fullMrc21DoneDir;
    String fullMrc21BadDir;
    
    /** 
     * Directory of MrcXML. This dir will be appended to the webdavBaseURL.
     */
    String fullMrcXmlDir;
    
    /** 
     * Directory of MrcNT. This dir will be appended to the webdavBaseURL.
     */
    String fullMrcNtDir;
    
    
    /* ******** properties used for Incremental Update ********* */
    String dailyMrcDir;
    String dailyMrcDoneDir;
    String dailyMrcBadDir;
    String dailyMrcDeleted;
    String dailyMrcXmlDir;
    String dailyMrcNtDir;
    String dailySuppressedDir;
    String dailyUnsuppressedDir;
        
    /**
     * URL of the solr service.
     */
    String solrUrl;
    

    /**
     * Load properties for VoyagerToBlacklightSolrConfiguration.
     * If both inA and inB are not null, inA will be loaded as defaults and inB will be
     * loaded as overrides to the values in inA.
     * 
     * If inB is null, only inA will be loaded. 
     */
    public static VoyagerToBlacklightSolrConfiguration loadFromPropertiesFile(InputStream inA, InputStream inB) 
            throws IOException{
        
        Properties prop;
        
        Properties propA = new Properties();
        //InputStream in = getClass().getResourceAsStream("//configuration.properties");                
        propA.load(inA);        
        inA.close();
        
        Properties propB = null; 
        if( inB != null ){
            propB = new Properties( propA );
            propB.load(inB);
            inB.close();
        }
        
        if( inB != null )
            prop = propA;
        else
            prop = propB;
        
        VoyagerToBlacklightSolrConfiguration conf = new VoyagerToBlacklightSolrConfiguration();

        conf.webdavBaseUrl = prop.getProperty("webdavBaseUrl");        
        
        conf.webdavUser = prop.getProperty("webdavUser");
        conf.webdavPassword = prop.getProperty( "webdavPassword");
        
        conf.fullMrc21Dir = prop.getProperty("fullMrc21Dir");        
        conf.fullMrc21DoneDir = prop.getProperty("fullMrc21DoneDir");
        conf.fullMrc21BadDir = prop.getProperty("fullMrc21BadDir");        
        conf.fullMrcXmlDir = prop.getProperty("fullMrcXmlDir");    
        conf.fullMrcNtDir = prop.getProperty("fullMrcNtDir");
        
        conf.dailyMrcDir = prop.getProperty("dailyMrcDir");
        conf.dailyMrcDoneDir = prop.getProperty("dailyMrcDoneDir");
        conf.dailyMrcBadDir = prop.getProperty("dailyMrcBadDir");
        conf.dailyMrcDeleted = prop.getProperty("dailyMrcDeleted");
        conf.dailyMrcXmlDir = prop.getProperty("dailyMrcXmlDir");
        conf.dailyMrcNtDir = prop.getProperty("dailyMrcNtDir");        
        conf.dailySuppressedDir = prop.getProperty("dailySuppressedDir");
        conf.dailyUnsuppressedDir = prop.getProperty("dailyUnsuppressedDir");
        
        conf.solrUrl = prop.getProperty("solrUrl");
        
        return conf;
    }

    /**
     * Returns empty String if configuration is good.
     * Otherwise, it returns a message describing what is missing or problematic.
     */
    public static String checkConfiguration( VoyagerToBlacklightSolrConfiguration checkMe){
        String errMsgs = "";

        errMsgs += checkWebdavUrl( checkMe.webdavBaseUrl );
    
        errMsgs += checkExits( checkMe.webdavUser , "webdavUser");
        errMsgs += checkExits( checkMe.webdavPassword , "webdavPassword");
        
        errMsgs += checkDir( checkMe.fullMrc21Dir, "fullMrc21Dir");    
        errMsgs += checkDir( checkMe.fullMrc21DoneDir, "fullMrc21DoneDir");
        errMsgs += checkDir( checkMe.fullMrc21BadDir, "fullMrc21BadDir");    
        errMsgs += checkDir( checkMe.fullMrcXmlDir, "fullMrcXmlDir");   
        errMsgs += checkDir( checkMe.fullMrcNtDir, "fullMrcNtDir");     
        errMsgs += checkDir( checkMe.dailyMrcDir, "dailyMrcDir");
        errMsgs += checkDir( checkMe.dailyMrcDoneDir, "dailyMrcDoneDir");
        errMsgs += checkDir( checkMe.dailyMrcBadDir, "dailyMrcBadDir");
        errMsgs += checkDir( checkMe.dailyMrcDeleted, "dailyMrcDeleted");
        errMsgs += checkDir( checkMe.dailyMrcXmlDir, "dailyMrcXmlDir");
        errMsgs += checkDir( checkMe.dailyMrcNtDir, "dailyMrcNtDir");
        errMsgs += checkDir( checkMe.dailySuppressedDir, "dailySuppressedDir");
        errMsgs += checkDir( checkMe.dailyUnsuppressedDir, "dailyUnsuppressedDir");

        errMsgs += checkSolrUrl( checkMe.solrUrl );
        return errMsgs;        
    }
    
    
    
    private static String checkExits(String value , String propName) {
       if( value == null || value.trim().isEmpty() )
           return "The property " + propName + " must not be empty or null.";
       else
           return "";
    }

    private static String checkSolrUrl(String solrUrl){
       if( solrUrl == null || solrUrl.trim().isEmpty() )
           return "The property solrUrl must be set.\n";
       else if( ! solrUrl.startsWith("http://"))
           return "The solrUrl was '" + solrUrl + "' but it must be a URL of the solr service.\n";
       else
           return "";
    }
    
    private static String checkWebdavUrl(String webdavBaseUrl){
        if( webdavBaseUrl == null || webdavBaseUrl.trim().isEmpty() )
            return "The property webdavBaseUrl must be set.\n";
        else if( ! webdavBaseUrl.startsWith("http://"))
            return "The webdavBaseUrl was '" + webdavBaseUrl + "' but it must be a URL of "
                    + "a directory in the WEBDAV service.\n";
        else
            return "";     
    }
    
    private static String checkDir( String dir, String propName ){
        String partEx = " It should be a directory part ex. voyager/bib.nt.daily\n";
        if( dir == null || dir.trim().isEmpty() )
            return "The property " + propName + " must not be empty." + partEx ;
        else if( dir.startsWith( "/" ) )
            return "The property " + propName + " must not start with a forward slash."
                    + partEx;
        else if( dir.startsWith("http://"))
            return "The property " + propName + " must not be a URL."
            + partEx;
        else
            return "";       
    }
}

