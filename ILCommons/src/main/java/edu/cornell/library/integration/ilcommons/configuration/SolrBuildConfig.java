package edu.cornell.library.integration.ilcommons.configuration;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;


/**
 * This is a basic structure intended to hold all the configuration information
 * needed for all steps of the Voyager MARC21 extract and convert to
 * Blacklight Solr index.
 * 
 * The goal is to facilitate the creation of dev, staging and production
 * configurations that can be called for from the command line of
 * the different steps of the conversion.
 * 
 * See the method loadConfig(String[]) for how the SolrBuildConf can
 * be loaded. 
 * 
 * If you'd like to add a new configuration setting to this class,
 *  1 add a property to this class
 *  2 add a getter to this class
 *  3 make sure your property is loaded in loadFromPropertiesFile
 *  4 make sure your property is checked in checkConfiguration
 *  5 add your property to the example properties file at voyagerToSolrConfig.properties.example
 */
public class SolrBuildConfig {

	protected static boolean debug = true;

	private Map<String,String> values = new HashMap<String,String>();
	private Map<String,ComboPooledDataSource> databases = new HashMap<String,ComboPooledDataSource>();
	private Map<String,RDFService> rdfservices = new HashMap<String,RDFService>();
    static DavService davService = null;

    
    public String getDailyBibUpdates() throws IOException {
    	if (values.containsKey("dailyBibUpdates")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibUpdates"));
    		return values.get("dailyBibUpdates");
    	} else {
    		return null;
    	}
    }

    public void setDailyBibUpdates(String str) {
        values.put("dailyBibUpdates", insertDate(str)) ;
    }

    public String getDailyBibDeletes() throws IOException {
    	if (values.containsKey("dailyBibDeletes")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibDeletes"));
    		return values.get("dailyBibDeletes");
    	} else {
    		return null;
    	}
    }
    public void setDailyBibDeletes(String str) {
        values.put("dailyBibDeletes", insertDate(str));
    }

    public String getDailyBibAdds() throws IOException {
    	if (values.containsKey("dailyBibAdds")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibAdds"));
    		return values.get("dailyBibAdds");
    	} else {
    		return null;
    	}
    }

    public String getWebdavBaseUrl() {
    	if (values.containsKey("webdavBaseUrl")) {
    		return values.get("webdavBaseUrl");
    	} else {
    		return null;
    	}
    }
    public String getLocalBaseFilePath() {
    	if (values.containsKey("localBaseFilePath")) {
    		return values.get("localBaseFilePath");
    	} else {
    		return null;
    	}
    }

    public String getWebdavUser() {
    	if (values.containsKey("webdavUser")) {
    		return values.get("webdavUser");
    	} else {
    		return null;
    	}
    }

    public String getWebdavPassword() {
    	if (values.containsKey("webdavPassword")) {
    		return values.get("webdavPassword");
    	} else {
    		return null;
    	}
    }

    public String getFullMrcBibDir() throws IOException {
    	if (values.containsKey("fullMrcBibDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullMrcBibDir"));
    		return values.get("fullMrcBibDir");
    	} else {
    		return null;
    	}
    }

    public String getFullMrcMfhdDir() throws IOException {
    	if (values.containsKey("fullMrcMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullMrcMfhdDir"));
    		return values.get("fullMrcMfhdDir");
    	} else {
    		return null;
    	}
    }

    public String getFullXmlBibDir() throws IOException {
    	if (values.containsKey("fullXmlBibDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullXmlBibDir"));
    		return values.get("fullXmlBibDir");
    	} else {
    		return null;
    	}
    }

    public String getFullXmlMfhdDir() throws IOException {
    	if (values.containsKey("fullXmlMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullXmlMfhdDir"));
    		return values.get("fullXmlMfhdDir");
    	} else {
    		return null;
    	}
    }

    public String getFullNtDir() throws IOException {
    	if (values.containsKey("fullNtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullNtDir"));
    		return values.get("fullNtDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdDir() throws IOException {
    	if (values.containsKey("dailyMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdDir"));
    		return values.get("dailyMfhdDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMrcDir() throws IOException {
    	if (values.containsKey("dailyMrcDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMrcDir"));
    		return values.get("dailyMrcDir");
    	} else {
    		return null;
    	}
    }
        
    public String getDailyBibMrcXmlDir() throws IOException {
    	if (values.containsKey("dailyBibMrcXmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibMrcXmlDir"));
    		return values.get("dailyBibMrcXmlDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdMrcXmlDir() throws IOException {
    	if (values.containsKey("dailyMfhdMrcXmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdMrcXmlDir"));
    		return values.get("dailyMfhdMrcXmlDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMrcNtDir() throws IOException {
    	if (values.containsKey("dailyMrcNtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMrcNtDir"));
    		return values.get("dailyMrcNtDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMrcNtFilenamePrefix() {
    	if (values.containsKey("dailyMrcNtFilenamePrefix")) {
    		return values.get("dailyMrcNtFilenamePrefix");
    	} else {
    		return null;
    	}
    }

    public String getDailyBibSuppressedDir() throws IOException {
    	if (values.containsKey("dailyBibSuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibSuppressedDir"));
    		return values.get("dailyBibSuppressedDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyBibSuppressedFilenamePrefix() {
    	if (values.containsKey("dailyBibSuppressedFilenamePrefix")) {
    		return values.get("dailyBibSuppressedFilenamePrefix");
    	} else {
    		return null;
    	}
    }

    public String getDailyBibUnsuppressedDir() throws IOException {
    	if (values.containsKey("dailyBibUnsuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibUnsuppressedDir"));
    		return values.get("dailyBibUnsuppressedDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyBibUnsuppressedFilenamePrefix() {
    	if (values.containsKey("dailyBibUnsuppressedFilenamePrefix")) {
    		return values.get("dailyBibUnsuppressedFilenamePrefix");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdSuppressedDir() throws IOException {
    	if (values.containsKey("dailyMfhdSuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdSuppressedDir"));
    		return values.get("dailyMfhdSuppressedDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdSuppressedFilenamePrefix() {
    	if (values.containsKey("dailyMfhdSuppressedFilenamePrefix")) {
    		return values.get("dailyMfhdSuppressedFilenamePrefix");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdUnsuppressedDir() throws IOException {
    	if (values.containsKey("dailyMfhdUnsuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdUnsuppressedDir"));
    		return values.get("dailyMfhdUnsuppressedDir");
    	} else {
    		return null;
    	}
    }

    public String getDailyMfhdUnsuppressedFilenamePrefix() {
    	if (values.containsKey("dailyMfhdUnsuppressedFilenamePrefix")) {
    		return values.get("dailyMfhdUnsuppressedFilenamePrefix");
    	} else {
    		return null;
    	}
    }
    
    public String[] getMarc2XmlDirs() throws IOException {
    	if (values.containsKey("marc2XmlDirs")) {
    		String[] aliases = values.get("marc2XmlDirs").split(",");
    		String[] paths = new String[ aliases.length ];
    		for (int i = 0; i < aliases.length; i++) {
    			if (values.containsKey(aliases[i])) 
    				paths[i] = values.get("webdavBaseUrl") + "/" + values.get(aliases[i]);
    			else {
    				System.out.println("Path not found for config value: " +aliases[i]);
    				return null;
    			}
    		}
    		return paths;
    	} 
    	return null;
    }
    
    public String getXmlDir() throws IOException {
    	if (values.containsKey("xmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("xmlDir"));
    		return values.get("xmlDir");
    	} else {
    		return null;
    	}
    }
    
    public String getNtDir() throws IOException {
    	if (values.containsKey("ntDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("ntDir"));
    		return values.get("ntDir");
    	} else {
    		return null;
    	}
    }

    public String getN3Dir() throws IOException {
    	if (values.containsKey("n3Dir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("n3Dir"));
    		return values.get("n3Dir");
    	} else {
    		return null;
    	}
    }

    public String getTxtDir() throws IOException {
    	if (values.containsKey("txtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("txtDir"));
    		return values.get("txtDir");
    	} else {
    		return null;
    	}
    }

    public String getTdfDir() throws IOException {
    	if (values.containsKey("tdfDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("tdfDir"));
    		return values.get("tdfDir");
    	} else {
    		return null;
    	}
    }

    public String getNonVoyUriPrefix() {
    	if (values.containsKey("nonVoyUriPrefix")) {
    		return values.get("nonVoyUriPrefix");
    	} else {
    		return null;
    	}
    }
    public String getNonVoyIdPrefix() {
    	if (values.containsKey("nonVoyIdPrefix")) {
    		return values.get("nonVoyIdPrefix");
    	} else {
    		return null;
    	}
    }
    public String getReportList() {
    	if (values.containsKey("reportList")) {
    		return values.get("reportList");
    	} else {
    		return null;
    	}
    }

    public String getSolrUrl() {
    	if (values.containsKey("solrUrl")) {
    		return values.get("solrUrl");
    	} else {
    		return null;
    	}
    }
    public String getBlacklightSolrUrl() {
    	if (values.containsKey("blacklightSolrUrl")) {
    		return values.get("blacklightSolrUrl");
    	} else {
    		return null;
    	}
    }
    
    /**
     * @return the tmpDir on local file system
     */
    public String getTmpDir() {
    	if (values.containsKey("tmpDir")) {
    		return values.get("tmpDir");
    	} else {
    		return null;
    	}
    }
    
    
   public RDFService getRDFService(String id) {
	   if (rdfservices.containsKey(id))
		   return rdfservices.get(id);
	   return null;
   }
   
   public void setRDFService( String id, RDFService rdf ) {
	   rdfservices.put(id, rdf);
   }
    
    
    /**
     * 
     * @param id : database identifier used in config properties file
     * @return java.sql.Connection
     * @throws ClassNotFoundException 
     * @throws SQLException 
     */
    public Connection getDatabaseConnection(String id) throws SQLException, ClassNotFoundException {
    	String driver =  values.get("databaseDriver"+id);
    	if (driver == null) {
    		System.out.println("Value not found for databaseDriver"+id);
    		for (String key : values.keySet())
    			System.out.println( key +": "+values.get(key));
    		System.exit(1);
    	}
    	String url = values.get("databaseURL"+id);
    	if (url == null) {
    		System.out.println("Value not found for databaseURL"+id);
    		System.exit(1);
    	}
    	String user = values.get("databaseUser"+id);
    	if (user == null) {
    		System.out.println("Value not found for databaseUser"+id);
    		System.exit(1);
    	}
    	String pass = values.get("databasePass"+id);
    	if (pass == null) {
    		System.out.println("Value not found for databasePass"+id);
    		System.exit(1);
    	}
    	
    	Boolean pooling = true; //default if not specified in config
    	if (values.containsKey("databasePooling"+id))
    		pooling = Boolean.valueOf( values.get("databasePooling"+id) );
    	
    	if (debug) System.out.println("Database connection pooling: "+pooling);
    	
    	if ( pooling )  {
	    	if ( ! databases.containsKey(id)) {
		    	ComboPooledDataSource cpds = new ComboPooledDataSource(); 
		    	try {
					cpds.setDriverClass( driver );
				} catch (PropertyVetoException e) {
					e.printStackTrace();
				}
		    	cpds.setJdbcUrl( url + "?useUnicode=true&characterEncoding=UTF-8" );
		    	cpds.setUser( user );
		    	cpds.setPassword( pass );
		    	cpds.setMaxStatements(25);
		    	cpds.setTestConnectionOnCheckout(true);
		    	cpds.setTestConnectionOnCheckin(true);
		    	// if we retry every thirty seconds for thirty attempts, we should be
		    	// able to handle 15 minutes of database downtime or network interruption.
		    	cpds.setAcquireRetryAttempts(30);
		    	cpds.setAcquireRetryDelay(30  * 1000); // s * ms/s
		    	cpds.setAcquireIncrement(1);
		    	cpds.setMinPoolSize(1);
		    	cpds.setMaxPoolSize(2);
		    	cpds.setInitialPoolSize(1);
		    	databases.put(id, cpds);
	    	}
	    	System.out.println("Connection pool established. Obtaining and returning connection.");
			return databases.get(id).getConnection();
    	} else {
        	Class.forName(driver);
 		   
        	if (debug) System.out.println("Establishing database connection.");
        	Connection c = DriverManager.getConnection(url,user,pass);
        	if (debug) System.out.println("database connection established.");
        	Statement stmt = c.createStatement();
        	stmt.executeUpdate("SET NAMES utf8");
        	return c;    		
    	}
    }


    public String getDailyReports() throws IOException {
    	if (values.containsKey("dailyReports")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyReports"));
    		return values.get("dailyReports");
    	} else {
    		return null;
    	}
    }
    
    public void setDailyReports(String reports){
        values.put("dailyReports", insertDate(reports));
    }
    
    /* Populate SolrBuildConfig values into Hadoop context mapper
     * 
     */
    public Mapper<Object,Text,Text,Text>.Context valuesToContext(Mapper<Object,Text,Text,Text>.Context context) {
    	for (String key : values.keySet()) 
    		context.getConfiguration().set(key, values.get(key));
    	return context;
    }
    
    /*
     * Convert Hadoop Configuration to SolrBuildConfig, inserting any dates into
     * config values where XXXX appears.
     */
    public static SolrBuildConfig loadConfig( Configuration hadoopConf ) {
        SolrBuildConfig solrBuildConfig = new SolrBuildConfig();
        Iterator<Map.Entry<String, String>> i = hadoopConf.iterator();
        while (i.hasNext()) {
            Map.Entry<String, String> entry = i.next();
            String field = entry.getKey();
            String value = entry.getValue();
            if (debug) System.out.println(field+" => "+value);
        	String valueWDate = insertDate(value);
        	if (debug) if ( ! value.equals(valueWDate)) System.out.println("\t\t==> "+valueWDate);
        	solrBuildConfig.values.put(field,valueWDate);
        }
    	return solrBuildConfig;
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
     *  argv may be null to force use of the environment variable. Should be 
     *  argv from main().
     * @throws Exception if no configuration is found or if there are problems with the configuration.
     */
    @Deprecated
    public static SolrBuildConfig loadConfig( String[] argv ) {
    	Collection<String> requiredFields = new HashSet<String>();
        return loadConfig(argv,requiredFields);        
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
     *  @param requiredFields is a task-specific Collection of field names required for the task.
     *  argv may be null to force use of the environment variable. Should be 
     *  argv from main().
     * @throws Exception if no configuration is found or if there are problems with the configuration.
     */
    public static SolrBuildConfig loadConfig( String[] argv, Collection<String> requiredFields ) {
        
        String v2bl_config = System.getenv(VOYAGER_TO_SOLR_CONFIG);
        
        if( v2bl_config != null &&  argv != null && argv.length > 0 )
            throw new RuntimeException( "Both command line arguments and the environment variable "
                    + VOYAGER_TO_SOLR_CONFIG + " are defined. It is unclear which to use.\n" + HELP );
        
        if( v2bl_config == null && ( argv == null || argv.length ==0 ))
            throw new RuntimeException("No configuration specified. \n"
                    + "A configuration is expeced on the command line or in the environment variable "
                    + VOYAGER_TO_SOLR_CONFIG + ".\n" + HELP );        
        
        SolrBuildConfig config=null;
        try{
        if( v2bl_config != null )
            config = loadFromEnvVar( v2bl_config );
        else
            config = loadFromArgv( argv );
        }catch( Exception ex){
            throw new RuntimeException("There were problems loading the configuration.\n " , ex);
        }
        
        String errs = checkConfiguration( requiredFields, config );
        if( errs == null ||  ! errs.trim().isEmpty() ){
            throw new RuntimeException("There were problems with the configuration.\n " + errs);
        }
        
        return config;        
    }


    private static SolrBuildConfig loadFromArgv(String[] argv) throws FileNotFoundException, IOException {
        if( argv.length > 1 ){   
            System.out.println("loading from command line arg: \n" + argv[0]);
            return loadFromPropertiesFile( getFile( argv[0]), null );
        }else{
            System.out.println("loading from command line arg: \n"+ argv[0] + "  " + argv[1] );
            return loadFromPropertiesFile( getFile(argv[0]), getFile(argv[1]));
        }            
    }


    /**
     * Load from the env var. It might be a single file name, or two file names seperated 
     * by a comma.  Also check the classpath.
     */
    private static SolrBuildConfig loadFromEnvVar( String value ) throws Exception {
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
    public static SolrBuildConfig loadFromPropertiesFile(InputStream inA, InputStream inB) 
            throws IOException{
        
        Properties prop;
        
        if (debug) System.out.println("Loading properties from file A.");
        Properties propA = new Properties();
        propA.load( inA );
        inA.close();
        
        if (debug) System.out.println("Loading properties from file B.");
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
        
        SolrBuildConfig conf = new SolrBuildConfig();

        if (debug) System.out.println("Adding all properties to program config.");
        Iterator<String> i = prop.stringPropertyNames().iterator();
        while (i.hasNext()) {
        	String field = i.next();
        	String value = prop.getProperty(field);
        	if (debug) System.out.println(field+" => "+value);
        	String valueWDate = insertDate(value);
        	if (debug) if ( ! value.equals(valueWDate)) System.out.println("\t\t==> "+valueWDate);
        	conf.values.put(field,valueWDate);
        	
        }

        return conf;
    }
    
    /**
     * 
     * @param String value - configuration value. 
     * @return if value contains "XXXX", returns string with "XXXX" replaced with current date
     * in YYYY-MM-DD format. If not, returns value unchanged.
     */
    public static String insertDate( String value ) {
    	if (value == null) {
    		// missing config value should be caught later.
    		return null;
    	}
    	if (value.contains("XXXX")) {
    		Calendar now = Calendar.getInstance();
    		String today = new SimpleDateFormat("yyyy-MM-dd").format(now.getTime());
    		return value.replace("XXXX", today);
    	} else {
    		return value;
    	}
    }
 
    /**
     * Returns empty String if configuration is good.
     * Otherwise, it returns a message describing what is missing or problematic.
     */
    public static String checkConfiguration( Collection<String> requiredArgs, SolrBuildConfig checkMe){
        String errMsgs = "";

        // universally required fields
        errMsgs += checkWebdavUrl( checkMe.values.get("webdavBaseUrl"));
        errMsgs += checkExists( checkMe.values.get("webdavUser") , "webdavUser");
        errMsgs += checkExists( checkMe.values.get("webdavPassword") , "webdavPassword");

        // fields required for select processes
        if (requiredArgs.contains("solrUrl"))
        	errMsgs += checkSolrUrl( checkMe.values.get("solrUrl") );
        if (requiredArgs.contains("blacklightSolrUrl"))
            errMsgs += checkSolrUrl( checkMe.values.get("blacklightSolrUrl") );
        if (requiredArgs.contains("fullMrcBibDir"))
        	errMsgs += checkDir( checkMe.values.get("fullMrcBibDir"), "fullMrcBibDir");
        if (requiredArgs.contains("fullMrcMfhdDir"))
        	errMsgs += checkDir( checkMe.values.get("fullMrcMfhdDir"), "fullMrcMfhdDir");
        if (requiredArgs.contains("fullXmlBibDir"))
        	errMsgs += checkDir( checkMe.values.get("fullXmlBibDir"), "fullXmlBibDir");
        if (requiredArgs.contains("fullXmlMfhdDir"))
        	errMsgs += checkDir( checkMe.values.get("fullXmlMfhdDir"), "fullXmlMfhdDir");
        if (requiredArgs.contains("fullNtBibDir"))
        	errMsgs += checkDir( checkMe.values.get("fullNtBibDir"), "fullNtBibDir");
        if (requiredArgs.contains("xmlDir"))
        	errMsgs += checkDir( checkMe.values.get("xmlDir"), "xmlDir");
        if (requiredArgs.contains("ntDir"))
        	errMsgs += checkDir( checkMe.values.get("ntDir"), "ntDir");
        if (requiredArgs.contains("n3Dir"))
        	errMsgs += checkDir( checkMe.values.get("n3Dir"), "n3Dir");
        if (requiredArgs.contains("txtDir"))
        	errMsgs += checkDir( checkMe.values.get("txtDir"), "txtDir");
        if (requiredArgs.contains("tdfDir"))
        	errMsgs += checkDir( checkMe.values.get("tdfDir"), "tdfDir");
        if (requiredArgs.contains("nonVoyUriPrefix"))
        	errMsgs += checkUriPrefix( checkMe.values.get("nonVoyUriPrefix"), "nonVoyUriPrefix");
        if (requiredArgs.contains("nonVoyIdPrefix"))
        	errMsgs += checkExists( checkMe.values.get("nonVoyIdPrefix"), "nonVoyIdPrefix");
        if (requiredArgs.contains("dailyBibUnsuppressedDir"))
        	errMsgs += checkExists( checkMe.values.get("dailyBibUnsuppressedDir"), "dailyBibUnsuppressedDir");
        if (requiredArgs.contains("dailyBibUnsuppressedFilenamePrefix"))
        	errMsgs += checkExists( checkMe.values.get("dailyBibUnsuppressedFilenamePrefix"), "dailyBibUnsuppressedFilenamePrefix");
        if (requiredArgs.contains("dailyMfhdUnsuppressedDir"))
        	errMsgs += checkExists( checkMe.values.get("dailyMfhdUnsuppressedDir"), "dailyMfhdUnsuppressedDir");
        if (requiredArgs.contains("dailyMfhdUnsuppressedFilenamePrefix"))
        	errMsgs += checkExists( checkMe.values.get("dailyMfhdUnsuppressedFilenamePrefix"), "dailyMfhdUnsuppressedFilenamePrefix");
        return errMsgs;        
    }


    private static String checkUriPrefix(String value, String propName) {
        if( value == null || value.trim().isEmpty() ){
            return "The property " + propName + " must not be empty or null.\n";
        }
        if ( ! value.startsWith("http") ) {
        	return "The property " + propName + " must start with \"http\".\n";
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
    
    private static String checkDir( String dir, String propName ){
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
    
    private void makeDirIfNeeded (String path) throws IOException{
    	if (davService == null)
    		davService = DavServiceFactory.getDavService(this);						
    	davService.mkDirRecursive( path );
    }
    
    protected static InputStream getFile(String name ) throws FileNotFoundException{
        File f = new File(name);
        if( f.exists() ){
            return new FileInputStream(f);
        }else{
            InputStream is = SolrBuildConfig.class.getClassLoader().getResourceAsStream(name);
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

