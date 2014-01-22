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
 * See the method loadConfig(String[]) for how the VoyagerToSolrConfiguration can
 * be loaded. 
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
    String dailyMrcDoneDir;
    String dailyMrcBadDir;
    String dailyBibMrcXmlDir;
    String dailyMfhdMrcXmlDir;
    String dailyMrcNtDir;
    String dailyMrcNtFilenamePrefix;
    String dailyBibSuppressedDir;
    String dailyBibSuppressedFilenamePrefix;
    String dailyBibUnsuppressedDir;
    String dailyBibUnsuppressedFilenamePrefix;
    String dailyMfhdSuppressedDir;
    String dailyMfhdSuppressedFilenamePrefix;
    String dailyMfhdUnsuppressedDir;
    String dailyMfhdUnsuppressedFilenamePrefix;
    String dailyBibUpdates; //= updates/bib.updates
    String dailyBibDeletes;
    String dailyReports;
    
    /**
     * URL of the solr service.
     */
    String solrUrl;
    
    String tmpDir;
    
    public String getDailyBibUpdates() {
        return dailyBibUpdates;
    }

    public void setDailyBibUpdates(String dailyBibUpdates) {
        this.dailyBibUpdates = dailyBibUpdates;
    }

    public String getDailyBibDeletes() {
        return dailyBibDeletes;
    }

    public void setDailyBibDeletes(String str) {
        this.dailyBibDeletes = str;
    }

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
     * @return the dailyBibMrcXmlDir
     */
    public String getDailyBibMrcXmlDir() {
        return dailyBibMrcXmlDir;
    }

    /**
     * @return the dailyMfhdMrcXmlDir
     */
    public String getDailyMfhdMrcXmlDir() {
        return dailyMfhdMrcXmlDir;
    }

    /**
     * @return the dailyMrcNtDir
     */
    public String getDailyMrcNtDir() {
        return dailyMrcNtDir;
    }

    /**
     * @return the dailyMrcNtFilenamePrefix
     */
    public String getDailyMrcNtFilenamePrefix() {
        return dailyMrcNtFilenamePrefix;
    }

    /**
     * @return the dailyBibSuppressedDir
     */
    public String getDailyBibSuppressedDir() {
        return dailyBibSuppressedDir;
    }

    /**
     * @return the dailyBibSuppressedFilenamePrefix
     */
    public String getDailyBibSuppressedFilenamePrefix() {
        return dailyBibSuppressedFilenamePrefix;
    }

    /**
     * @return the dailyBibUnsuppressedDir
     */
    public String getDailyBibUnsuppressedDir() {
        return dailyBibUnsuppressedDir;
    }

    /**
     * @return the dailyBibUnsuppressedFilenamePrefix
     */
    public String getDailyBibUnsuppressedFilenamePrefix() {
        return dailyBibUnsuppressedFilenamePrefix;
    }

    /**
     * @return the dailyMfhdSuppressedDir
     */
    public String getDailyMfhdSuppressedDir() {
        return dailyMfhdSuppressedDir;
    }

    /**
     * @return the dailyMfhdSuppressedFilenamePrefix
     */
    public String getDailyMfhdSuppressedFilenamePrefix() {
        return dailyMfhdSuppressedFilenamePrefix;
    }

    /**
     * @return the dailyMfhdUnsuppressedDir
     */
    public String getDailyMfhdUnsuppressedDir() {
        return dailyMfhdUnsuppressedDir;
    }

    /**
     * @return the dailyMfhdUnsuppressedFilenamePrefix
     */
    public String getDailyMfhdUnsuppressedFilenamePrefix() {
        return dailyMfhdUnsuppressedFilenamePrefix;
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

    public String getDailyReports(){
        return dailyReports;
    }
    
    public void setDailyReports(String reports){
        dailyReports = reports;
    }
    
    /**
     * A utility method to load properties from command line or environment.
     * 
     * Configured jobs on integration servers or automation systems are
     * expected to use the environment variable VOYAGER_TO_SOLR_CONFIG to indicate the property
     * file to use.
     * 
     * Development jobs are expected to use command line arguments.
     *  
     * If properties files exist on the command line use those, 
     * If the environment variable VOYAGER_TO_SOLR_CONFIG exists use those,
     * If both environment variable VOYAGER_TO_SOLR_CONFIG and command line arguments exist, 
     * throw an error because that is a confused state and likely a problem.
     *  
     * The value of the environment variable VOYAGER_TO_SOLR_CONFIG may be a comma separated list of files.
     * 
     *  @param argv may be null to force use of the environment variable. Should be 
     *  argv from main().
     * @throws Exception if no configuration is found or if there are problems with the configuration.
     */
    public static VoyagerToSolrConfiguration loadConfig( String[] argv ) {
        
        String v2bl_config = System.getenv(VOYAGER_TO_SOLR_CONFIG);
        
        if( v2bl_config != null &&  argv != null && argv.length > 0 )
            throw new RuntimeException( "Both command line arguments and the environment variable "
                    + VOYAGER_TO_SOLR_CONFIG + " are defined. It is unclear which to use.\n" + HELP );
        
        if( v2bl_config == null && ( argv == null || argv.length ==0 ))
            throw new RuntimeException("No configuration specified. \n"
                    + "A configuration is expeced on the command line or in the environment variable "
                    + VOYAGER_TO_SOLR_CONFIG + ".\n" + HELP );        
        
        VoyagerToSolrConfiguration config=null;
        try{
        if( v2bl_config != null )
            config = loadFromEnvVar( v2bl_config );
        else
            config = loadFromArgv( argv );
        }catch( Exception ex){
            throw new RuntimeException("There were problems loading the configuration.\n " , ex);
        }
        
        String errs = checkConfiguration( config );
        if( errs == null ||  ! errs.trim().isEmpty() ){
            throw new RuntimeException("There were problems with the configuration.\n " + errs);
        }
        
        return config;        
    }

    private static VoyagerToSolrConfiguration loadFromArgv(
            String[] argv) throws FileNotFoundException, IOException {
        if( argv.length > 1 ){   
            System.out.println("loading from command line arg: \n" 
                    + argv[0]);
            return loadFromPropertiesFile( getFile( argv[0]), null );
        }else{
            System.out.println("loading from command line arg: \n" 
                    + argv[0] + "  " + argv[1] );
            return loadFromPropertiesFile( getFile(argv[0]), getFile(argv[1]));
        }            
    }


    /**
     * Load from the env var. It might be a single file name, or two file names seperated 
     * by a comma.  Also check the classpath.
     */
    private static VoyagerToSolrConfiguration loadFromEnvVar( String value ) throws Exception {
        System.out.println("loading from environment variable '" + VOYAGER_TO_SOLR_CONFIG + "'="+value);

        if( ! value.contains(",") ){
            return loadFromPropertiesFile( getFile( value ), null );
        }else{
//            String firstFileName, secondFileName;
            String names[] = value.split(",");
            if( names.length > 2 )
                throw new Exception("The env var has more than two files: " + value);
            if( names.length < 2 )
                throw new Exception("The env var has fewer than two files: " + value);
            return loadFromPropertiesFile( getFile(names[0]), getFile(names[1]));
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
        propA.load( inA );
        inA.close();
        
        Properties propB = null; 
        if( inB != null ){
            propB = new Properties( );
            propB.load(inB);
            inB.close();
        }
        
        if( inB != null ){
            prop = new Properties();
            prop.putAll( propB );
            prop.putAll( propA );
        } else {
            prop = propB;
        }
        
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
        conf.dailyMrcDoneDir = prop.getProperty("dailyMrcDoneDir");
        conf.dailyMrcBadDir = prop.getProperty("dailyMrcBadDir");
        conf.dailyBibMrcXmlDir = prop.getProperty("dailyBibMrcXmlDir");
        conf.dailyMfhdMrcXmlDir = prop.getProperty("dailyMfhdMrcXmlDir");
        conf.dailyMrcNtDir = prop.getProperty("dailyMrcNtDir");
        conf.dailyMrcNtFilenamePrefix = prop.getProperty("dailyMrcNtFilenamePrefix");
        conf.dailyBibSuppressedDir = prop.getProperty("dailyBibSuppressedDir");
        conf.dailyBibSuppressedFilenamePrefix = prop.getProperty("dailyBibSuppressedFilenamePrefix");
        conf.dailyBibUnsuppressedDir = prop.getProperty("dailyBibUnsuppressedDir");
        conf.dailyBibUnsuppressedFilenamePrefix = prop.getProperty("dailyBibUnsuppressedFilenamePrefix");
        conf.dailyMfhdSuppressedDir = prop.getProperty("dailyMfhdSuppressedDir");
        conf.dailyMfhdSuppressedFilenamePrefix = prop.getProperty("dailyMfhdSuppressedFilenamePrefix");
        conf.dailyMfhdUnsuppressedDir = prop.getProperty("dailyMfhdUnsuppressedDir");
        conf.dailyMfhdUnsuppressedFilenamePrefix = prop.getProperty("dailyMfhdUnsuppressedFilenamePrefix");
        conf.dailyBibDeletes = prop.getProperty( "dailyBibDelets");
        conf.dailyBibUpdates = prop.getProperty("dailyBibUpdates");

        conf.dailyBibDeletes = prop.getProperty( "dailyBibDeletes");

        conf.dailyReports = prop.getProperty("dailyReports");
        
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
    
        errMsgs += checkExists( checkMe.webdavUser , "webdavUser");
        errMsgs += checkExists( checkMe.webdavPassword , "webdavPassword");
        
        errMsgs += checkWebdavDir( checkMe.fullMrc21Dir, "fullMrc21Dir");    
        errMsgs += checkWebdavDir( checkMe.fullMrc21DoneDir, "fullMrc21DoneDir");
        errMsgs += checkWebdavDir( checkMe.fullMrc21BadDir, "fullMrc21BadDir");    
        errMsgs += checkWebdavDir( checkMe.fullMrcXmlDir, "fullMrcXmlDir");   
        errMsgs += checkWebdavDir( checkMe.fullMrcNtDir, "fullMrcNtDir");     
        errMsgs += checkWebdavDir( checkMe.dailyMrcDir, "dailyMrcDir");        
        errMsgs += checkWebdavDir( checkMe.dailyMfhdDir, "dailyMfhdDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcDoneDir, "dailyMrcDoneDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcBadDir, "dailyMrcBadDir");
        errMsgs += checkWebdavDir( checkMe.dailyBibMrcXmlDir, "dailyBibMrcXmlDir");
        errMsgs += checkWebdavDir( checkMe.dailyMfhdMrcXmlDir, "dailyMfhdMrcXmlDir");
        errMsgs += checkWebdavDir( checkMe.dailyMrcNtDir, "dailyMrcNtDir");
        errMsgs += checkExists(    checkMe.dailyMrcNtFilenamePrefix, "dailyMrcNtFilenamePrefix");
        errMsgs += checkWebdavDir( checkMe.dailyBibSuppressedDir, "dailyBibSuppressedDir");
        errMsgs += checkExists(    checkMe.dailyBibSuppressedFilenamePrefix, "dailyBibSuppressedFilenamePrefix");
        errMsgs += checkWebdavDir( checkMe.dailyBibUnsuppressedDir, "dailyBibUnsuppressedDir");
        errMsgs += checkExists(    checkMe.dailyBibUnsuppressedFilenamePrefix, "dailyBibUnsuppressedFilenamePrefix");
        errMsgs += checkWebdavDir( checkMe.dailyMfhdSuppressedDir, "dailyMfhdSuppressedDir");
        errMsgs += checkExists(    checkMe.dailyMfhdSuppressedFilenamePrefix, "dailyMfhdSuppressedFilenamePrefix");
        errMsgs += checkWebdavDir( checkMe.dailyMfhdUnsuppressedDir, "dailyMfhdUnsuppressedDir");
        errMsgs += checkExists(    checkMe.dailyMfhdUnsuppressedFilenamePrefix, "dailyMfhdUnsuppressedFilenamePrefix");
        errMsgs += checkWebdavDir( checkMe.dailyBibDeletes,  "dailyBibDeletes");
        errMsgs += checkWebdavDir( checkMe.dailyBibUpdates, "dailyBibUpdates");
        errMsgs += checkWebdavDir( checkMe.dailyReports,    "dailyReports");
        
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

    private static String checkExists(String value , String propName) {
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
        String partEx = " It should be a directory part with no leading or trailing slash ex. voyager/bib.nt.daily \n";
        if( dir == null || dir.trim().isEmpty() )
            return "The property " + propName + " must not be empty." + partEx ;
        else if( dir.startsWith( "/" ) )
            return "The property " + propName + " must not start with a forward slash."
                    + partEx;
        else if( dir.endsWith("/"))
            return "The property " + propName + " must not end with a forward slash."
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
    public final static String VOYAGER_TO_SOLR_CONFIG = "VoyagerToSolrConfig";
    
    public final static String HELP = 
            "On the command line the first two parameters may be configuration property files.\n"+
            "Ex. java someClass staging.properties dav.properties \n" +
            "Or the environment variable VoyagerToSolrConfig can be set to one or two properties files:\n" +
            "Ex. VoyagerToSolrConfig=prod.properties,prodDav.properties java someClass\n" +
            "Do not use both a environment variable and command line parameters.\n" +
            "These files will be searched for first in the file system, then from the classpath/ClassLoader.\n";

    
}

