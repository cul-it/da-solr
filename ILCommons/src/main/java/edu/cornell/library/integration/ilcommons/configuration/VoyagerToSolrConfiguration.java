package edu.cornell.library.integration.ilcommons.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
 * If you'd like to add a new configuration setting to this class,
 *  1 add a property to this class
 *  2 add a getter to this class
 *  3 make sure your property is loaded in loadFromPropertiesFile
 *  4 make sure your property is checked in checkConfiguration
 *  5 add your property to the example properties file at voyagerToSolrConfig.properties.example
 */
public class VoyagerToSolrConfiguration {
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
    String dailyMfhdDir;
    String dailyCombinedMrcDir;
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
    
    String tmpDir;
    
    /**
     * @return the webdavBaseUrl
     */
    public String getWebdavBaseUrl() {
        return webdavBaseUrl;
    }

    /**
     * @return the webdavUser
     */
    public String getWebdavUser() {
        return webdavUser;
    }

    /**
     * @return the webdavPassword
     */
    public String getWebdavPassword() {
        return webdavPassword;
    }

    /**
     * @return the fullMrc21Dir
     */
    public String getFullMrc21Dir() {
        return fullMrc21Dir;
    }

    /**
     * @return the fullMrc21DoneDir
     */
    public String getFullMrc21DoneDir() {
        return fullMrc21DoneDir;
    }

    /**
     * @return the fullMrc21BadDir
     */
    public String getFullMrc21BadDir() {
        return fullMrc21BadDir;
    }

    /**
     * @return the fullMrcXmlDir
     */
    public String getFullMrcXmlDir() {
        return fullMrcXmlDir;
    }

    /**
     * @return the fullMrcNtDir
     */
    public String getFullMrcNtDir() {
        return fullMrcNtDir;
    }

    public String getDailyMfhdDir() {
        return this.dailyMfhdDir;
    }

    /**
     * @return the dailyMrcDir
     */
    public String getDailyMrcDir() {
        return dailyMrcDir;
    }


    public String getDailyCombinedMrcDir() {   
        return this.dailyCombinedMrcDir;
    }
        
    /**
     * @return the dailyMrcDoneDir
     */
    public String getDailyMrcDoneDir() {
        return dailyMrcDoneDir;
    }

    /**
     * @return the dailyMrcBadDir
     */
    public String getDailyMrcBadDir() {
        return dailyMrcBadDir;
    }

    /**
     * @return the dailyMrcDeleted
     */
    public String getDailyMrcDeleted() {
        return dailyMrcDeleted;
    }

    /**
     * @return the dailyMrcXmlDir
     */
    public String getDailyMrcXmlDir() {
        return dailyMrcXmlDir;
    }

    /**
     * @return the dailyMrcNtDir
     */
    public String getDailyMrcNtDir() {
        return dailyMrcNtDir;
    }

    /**
     * @return the dailySuppressedDir
     */
    public String getDailySuppressedDir() {
        return dailySuppressedDir;
    }

    /**
     * @return the dailyUnsuppressedDir
     */
    public String getDailyUnsuppressedDir() {
        return dailyUnsuppressedDir;
    }

    /**
     * @return the solrUrl
     */
    public String getSolrUrl() {
        return solrUrl;
    }
    
    /**
     * @return the tmpDir
     */
    public String getTmpDir() {
      return tmpDir;
    }

    /**
     * A utility method to load properties from command line or environment.
     * 
     * Configured jobs on integration servers or automation systems are
     * expected to use the environment variable V2BL_CONFIG to indicate the property
     * file to use.
     * 
     * Development jobs are expected to use command line arguments.
     *  
     * If properties files exist on the command line use those, 
     * If the environment variable V2BL_CONFIG exists use those,
     * If both environment variable V2BL_CONFIG and command line arguments exist, 
     * throw an error because that is a confused state and likely a problem.
     *  
     * The value of the environment variable V2BL_CONFIG may be a comma separated list of files.
     * 
     *  @param argv may be null to force use of the environment variable. Should be 
     *  argv from main().
     * @throws Exception if no configuration is found or if there are problems with the configuration.
     */
    public static VoyagerToSolrConfiguration loadConfig( String[] argv ) {
        
        String v2bl_config = System.getenv(V2S_CONFIG);
        
        if( v2bl_config != null &&  argv != null && argv.length > 0 )
            throw new RuntimeException( "Both command line arguments and the environment variable "
                    + "V2BL_CONFIG are defined. It is unclear which to use.\n" + HELP );
        
        if( v2bl_config == null && ( argv == null || argv.length ==0 ))
            throw new RuntimeException("No configuration specified. \n"
                    + "A configuration is expeced on the command line or in the environment variable "
                    + "V2BL_CONFIG.\n" + HELP );        
        
        VoyagerToSolrConfiguration config=null;
        try{
        if( v2bl_config != null )
            config = loadFromPropertiesFile( getFile( v2bl_config ), null);            
        else
            config = loadFromArgv( argv );
        }catch( Exception ex){
            throw new RuntimeException("There were problems loading the configuration.\n " , ex);
        }
        
        String errs = checkConfiguration( config );
        if( errs != null || errs.trim().isEmpty() ){
            throw new RuntimeException("There were problems with the configuration.\n " + errs);
        }
        
        return config;        
    }

    private static VoyagerToSolrConfiguration loadFromArgv(
            String[] argv) throws FileNotFoundException, IOException {
        if( argv.length > 1 ){            
            return loadFromPropertiesFile( getFile( argv[0]), null );
        }else{
            return loadFromPropertiesFile( getFile(argv[0]), getFile(argv[1]));
        }            
    }

    /**
     * Load properties for VoyagerToBlacklightSolrConfiguration.
     * If both inA and inB are not null, inA will be loaded as defaults and inB will be
     * loaded as overrides to the values in inA.
     * 
     * If inB is null, only inA will be loaded. 
     */
    public static VoyagerToSolrConfiguration loadFromPropertiesFile(InputStream inA, InputStream inB) 
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
        
        VoyagerToSolrConfiguration conf = new VoyagerToSolrConfiguration();

        conf.webdavBaseUrl = prop.getProperty("webdavBaseUrl");        
        
        conf.webdavUser = prop.getProperty("webdavUser");
        conf.webdavPassword = prop.getProperty( "webdavPassword");
        
        conf.fullMrc21Dir = prop.getProperty("fullMrc21Dir");        
        conf.fullMrc21DoneDir = prop.getProperty("fullMrc21DoneDir");
        conf.fullMrc21BadDir = prop.getProperty("fullMrc21BadDir");        
        conf.fullMrcXmlDir = prop.getProperty("fullMrcXmlDir");    
        conf.fullMrcNtDir = prop.getProperty("fullMrcNtDir");
        
        conf.dailyMrcDir = prop.getProperty("dailyMrcDir");
        conf.dailyMfhdDir = prop.getProperty("dailyMfhdDir");
        conf.dailyCombinedMrcDir = prop.getProperty("dailyCombinedMrcDir");
        conf.dailyMrcDoneDir = prop.getProperty("dailyMrcDoneDir");
        conf.dailyMrcBadDir = prop.getProperty("dailyMrcBadDir");
        conf.dailyMrcDeleted = prop.getProperty("dailyMrcDeleted");
        conf.dailyMrcXmlDir = prop.getProperty("dailyMrcXmlDir");
        conf.dailyMrcNtDir = prop.getProperty("dailyMrcNtDir");        
        conf.dailySuppressedDir = prop.getProperty("dailySuppressedDir");
        conf.dailyUnsuppressedDir = prop.getProperty("dailyUnsuppressedDir");
        
        conf.solrUrl = prop.getProperty("solrUrl");
        
        conf.tmpDir = prop.getProperty("tmpDir");
        
        return conf;
    }

    /**
     * Returns empty String if configuration is good.
     * Otherwise, it returns a message describing what is missing or problematic.
     */
    public static String checkConfiguration( VoyagerToSolrConfiguration checkMe){
        String errMsgs = "";

        errMsgs += checkWebdavUrl( checkMe.webdavBaseUrl );
    
        errMsgs += checkExits( checkMe.webdavUser , "webdavUser");
        errMsgs += checkExits( checkMe.webdavPassword , "webdavPassword");
        
        errMsgs += checkWebdavDir( checkMe.fullMrc21Dir, "fullMrc21Dir");    
        errMsgs += checkWebdavDir( checkMe.fullMrc21DoneDir, "fullMrc21DoneDir");
        errMsgs += checkWebdavDir( checkMe.fullMrc21BadDir, "fullMrc21BadDir");    
        errMsgs += checkWebdavDir( checkMe.fullMrcXmlDir, "fullMrcXmlDir");   
        errMsgs += checkWebdavDir( checkMe.fullMrcNtDir, "fullMrcNtDir");     
        errMsgs += checkWebdavDir( checkMe.dailyMrcDir, "dailyMrcDir");        
        errMsgs += checkWebdavDir( checkMe.dailyMfhdDir, "dailyMfhdDir");
        errMsgs += checkWebdavDir( checkMe.dailyCombinedMrcDir, "dailyCombinedMrcDir");        
        errMsgs += checkWebdavDir( checkMe.dailyMrcDoneDir, "dailyMrcDoneDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcBadDir, "dailyMrcBadDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcDeleted, "dailyMrcDeleted");
        errMsgs += checkWebdavDir( checkMe.dailyMrcXmlDir, "dailyMrcXmlDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcNtDir, "dailyMrcNtDir");
        errMsgs += checkWebdavDir( checkMe.dailySuppressedDir, "dailySuppressedDir");
        errMsgs += checkWebdavDir( checkMe.dailyUnsuppressedDir, "dailyUnsuppressedDir");

        errMsgs += checkSolrUrl( checkMe.solrUrl );
        
        errMsgs += checkDir( checkMe.tmpDir, "tmpDir");
        return errMsgs;        
    }
    
    
    
    private static String checkDir(String value, String propName) {
        if( value == null || value.trim().isEmpty() ){
            return "The property " + propName + " must not be empty or null.\n";
        }
        return "";
    }

    private static String checkExits(String value , String propName) {
       if( value == null || value.trim().isEmpty() )
           return "The property " + propName + " must not be empty or null.\n";
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
    
    private static String checkWebdavDir( String dir, String propName ){
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
    
    protected static InputStream getFile(String name ) throws FileNotFoundException{
        File f = new File(name);
        if( f.exists() ){
            return new FileInputStream(f);
        }else{
            InputStream is = VoyagerToSolrConfiguration.class.getClassLoader().getResourceAsStream(name);
            if( is == null )
                throw new FileNotFoundException("Could not find file in file system or on classpath: " + name );
            else
                return is;
        }                        
    }
    
    
    /**
     * Name of environment variable for configuration files.
     */
    public final static String V2S_CONFIG = "VoyagerToSolrConfig";
    
    public final static String HELP = 
            "On the command line the first two parameters may be configuration property files.\n"+
            "Ex. java someClass staging.v2bl.properties dav.properties \n" +
            "Or the environment variable V2BL_CONFIG can be set to one or two properties files:\n" +
            "Ex. V2BL_CONFIG=prod.v2bl.properties,prodDav.properties java someClass\n" +
            "Do not use both a environment variable and command line parameters.\n" +
            "These files will be searched for first in the file system, then from the classpath/ClassLoader.\n";

    
}
