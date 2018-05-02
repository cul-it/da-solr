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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.ConfigurationException;

import org.apache.hadoop.conf.Configuration;

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

	protected static boolean debug = false;

	private Map<String,String> values = new HashMap<>();
	private Map<String,ComboPooledDataSource> databases = new HashMap<>();
	private Map<String,RDFService> rdfservices = new HashMap<>();
    static DavService davService = null;
    private List<Class<?>> debugRSTFs = new ArrayList<>();
    
    public static List<String> getRequiredArgsForDB( String db ) {
    	List<String> list = new ArrayList<>();
    	if (db == null) return null;
    	if (db.isEmpty()) return null;
    	list.add("DatabaseDriver"+db);
    	list.add("DatabaseURL"+db);
    	list.add("DatabaseUser"+db);
    	list.add("DatabasePass"+db);
    	return list;
    }

    public static List<String> getRequiredArgsForWebdav() {
    	List<String> list = new ArrayList<>();
    	list.add("webdavBaseUrl");
    	list.add("webdavUser");
    	list.add("webdavPassword");
    	return list;
    }
    
    public void setDebugRSTFClass( Class<?> c ) {
    	debugRSTFs.add(c);
    }
    public boolean isDebugClass( Class<?> c ) {
    	return debugRSTFs.contains(c);
    }
    
    public String getDailyBibUpdates() throws IOException {
    	if (values.containsKey("dailyBibUpdates")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibUpdates"));
    		return values.get("dailyBibUpdates");
    	}
		return null;
    }

    public void setDailyBibUpdates(String str) {
        values.put("dailyBibUpdates", insertIterationContext(str)) ;
    }

    public String getDailyBibDeletes() throws IOException {
    	if (values.containsKey("dailyBibDeletes")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibDeletes"));
    		return values.get("dailyBibDeletes");
    	}
		return null;
    }
    public void setDailyBibDeletes(String str) {
        values.put("dailyBibDeletes", insertIterationContext(str));
    }

    public String getDailyBibAdds() throws IOException {
    	if (values.containsKey("dailyBibAdds")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibAdds"));
    		return values.get("dailyBibAdds");
    	}
		return null;
    }

    public String getWebdavBaseUrl() {
    	if (values.containsKey("webdavBaseUrl")) {
    		return values.get("webdavBaseUrl");
    	}
		return null;
    }
    public void setWebdavBaseUrl(String url) {
    	values.put("webdavBaseUrl",url);
    }

    public String getLocalBaseFilePath() {
    	if (values.containsKey("localBaseFilePath")) {
    		return values.get("localBaseFilePath");
    	}
		return null;
    }
    public void setLocalBaseFilePath(String path) {
    		values.put("localBaseFilePath",path);
    }

    public String getWebdavUser() {
    	if (values.containsKey("webdavUser")) {
    		return values.get("webdavUser");
    	}
		return null;
    }

    public String getWebdavPassword() {
    	if (values.containsKey("webdavPassword")) {
    		return values.get("webdavPassword");
    	}
		return null;
    }

    public String getLocationDir() throws IOException {
    	if (values.containsKey("locationDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("locationDir"));
    		return values.get("locationDir");
    	}
		return null;
    }

    public String getBatchInfoDir() {
    	if (values.containsKey("batchInfoDir")) {
    		return values.get("batchInfoDir");
    	}
		return null;
    }

    public String getFullMrcBibDir() throws IOException {
    	if (values.containsKey("fullMrcBibDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullMrcBibDir"));
    		return values.get("fullMrcBibDir");
    	}
		return null;
    }

    public String getFullMrcMfhdDir() throws IOException {
    	if (values.containsKey("fullMrcMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullMrcMfhdDir"));
    		return values.get("fullMrcMfhdDir");
    	}
		return null;
    }

    public String getFullXmlBibDir() throws IOException {
    	if (values.containsKey("fullXmlBibDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullXmlBibDir"));
    		return values.get("fullXmlBibDir");
    	}
		return null;
    }

    public String getFullXmlMfhdDir() throws IOException {
    	if (values.containsKey("fullXmlMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullXmlMfhdDir"));
    		return values.get("fullXmlMfhdDir");
    	}
		return null;
    }

    public String getFullNtDir() throws IOException {
    	if (values.containsKey("fullNtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("fullNtDir"));
    		return values.get("fullNtDir");
    	}
		return null;
    }

    public String getDailyMfhdDir() throws IOException {
    	if (values.containsKey("dailyMfhdDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdDir"));
    		return values.get("dailyMfhdDir");
    	}
		return null;
    }

    public String getDailyMrcDir() throws IOException {
    	if (values.containsKey("dailyMrcDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMrcDir"));
    		return values.get("dailyMrcDir");
    	}
		return null;
    }
        
    public String getDailyBibMrcXmlDir() throws IOException {
    	if (values.containsKey("dailyBibMrcXmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibMrcXmlDir"));
    		return values.get("dailyBibMrcXmlDir");
    	}
		return null;
    }

    public String getDailyMfhdMrcXmlDir() throws IOException {
    	if (values.containsKey("dailyMfhdMrcXmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdMrcXmlDir"));
    		return values.get("dailyMfhdMrcXmlDir");
    	}
		return null;
    }

    public String getDailyMrcNtDir() throws IOException {
    	if (values.containsKey("dailyMrcNtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMrcNtDir"));
    		return values.get("dailyMrcNtDir");
    	}
		return null;
    }

    public String getDailyMrcNtFilenamePrefix() {
    	if (values.containsKey("dailyMrcNtFilenamePrefix")) {
    		return values.get("dailyMrcNtFilenamePrefix");
    	}
		return null;
    }

    public String getDailyBibSuppressedDir() throws IOException {
    	if (values.containsKey("dailyBibSuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibSuppressedDir"));
    		return values.get("dailyBibSuppressedDir");
    	}
		return null;
    }

    public String getDailyBibSuppressedFilenamePrefix() {
    	if (values.containsKey("dailyBibSuppressedFilenamePrefix")) {
    		return values.get("dailyBibSuppressedFilenamePrefix");
    	}
		return null;
    }

    public String getDailyBibUnsuppressedDir() throws IOException {
    	if (values.containsKey("dailyBibUnsuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyBibUnsuppressedDir"));
    		return values.get("dailyBibUnsuppressedDir");
    	}
		return null;
    }

    public String getDailyBibUnsuppressedFilenamePrefix() {
    	if (values.containsKey("dailyBibUnsuppressedFilenamePrefix")) {
    		return values.get("dailyBibUnsuppressedFilenamePrefix");
    	}
		return null;
    }

    public String getDailyItemDir() throws IOException {
    	if (values.containsKey("dailyItemDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyItemDir"));
    		return values.get("dailyItemDir");
    	}
		return null;
    }

    public String getDailyItemFilenamePrefix() {
    	if (values.containsKey("dailyItemFilenamePrefix")) {
    		return values.get("dailyItemFilenamePrefix");
    	}
		return null;
    }

    public String getDailyMfhdSuppressedDir() throws IOException {
    	if (values.containsKey("dailyMfhdSuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdSuppressedDir"));
    		return values.get("dailyMfhdSuppressedDir");
    	}
		return null;
    }

    public String getDailyMfhdSuppressedFilenamePrefix() {
    	if (values.containsKey("dailyMfhdSuppressedFilenamePrefix")) {
    		return values.get("dailyMfhdSuppressedFilenamePrefix");
    	}
		return null;
    }

    public String getDailyMfhdUnsuppressedDir() throws IOException {
    	if (values.containsKey("dailyMfhdUnsuppressedDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyMfhdUnsuppressedDir"));
    		return values.get("dailyMfhdUnsuppressedDir");
    	}
		return null;
    }

    public String getDailyMfhdUnsuppressedFilenamePrefix() {
    	if (values.containsKey("dailyMfhdUnsuppressedFilenamePrefix")) {
    		return values.get("dailyMfhdUnsuppressedFilenamePrefix");
    	}
		return null;
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
    		for (String path : paths)
    			makeDirIfNeeded( path );
    		return paths;
    	} 
    	return null;
    }

    public String getXmlDir() throws IOException {
    	if (values.containsKey("xmlDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("xmlDir"));
    		return values.get("xmlDir");
    	}
		return null;
    }

    public String getMrcDir() throws IOException {
    	if (values.containsKey("mrcDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("mrcDir"));
    		return values.get("mrcDir");
    	}
		return null;
    }

    public String getNtDir() throws IOException {
    	if (values.containsKey("ntDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("ntDir"));
    		return values.get("ntDir");
    	}
		return null;
    }

    public String getN3Dir() throws IOException {
    	if (values.containsKey("n3Dir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("n3Dir"));
    		return values.get("n3Dir");
    	}
		return null;
    }

    public String getTxtDir() throws IOException {
    	if (values.containsKey("txtDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("txtDir"));
    		return values.get("txtDir");
    	}
		return null;
    }

    public String getTdfDir() throws IOException {
    	if (values.containsKey("tdfDir")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("tdfDir"));
    		return values.get("tdfDir");
    	}
		return null;
    }

    public String getNonVoyUriPrefix() {
    	if (values.containsKey("nonVoyUriPrefix")) {
    		return values.get("nonVoyUriPrefix");
    	}
		return null;
    }
    public String getNonVoyIdPrefix() {
    	if (values.containsKey("nonVoyIdPrefix")) {
    		return values.get("nonVoyIdPrefix");
    	}
		return null;
    }
    public String getReportList() {
    	if (values.containsKey("reportList")) {
    		return values.get("reportList");
    	}
		return null;
    }

    public String getSolrUrl() {
    	if (values.containsKey("solrUrl")) {
    		return values.get("solrUrl");
    	}
		return null;
    }
    public String getAuthorSolrUrl() {
    	if (values.containsKey("authorSolrUrl")) {
    		return values.get("authorSolrUrl");
    	}
		return null;
    }
    public String getAuthorTitleSolrUrl() {
    	if (values.containsKey("authorTitleSolrUrl")) {
    		return values.get("authorTitleSolrUrl");
    	}
		return null;
    }
    public String getSubjectSolrUrl() {
    	if (values.containsKey("subjectSolrUrl")) {
    		return values.get("subjectSolrUrl");
    	}
		return null;
    }
    public String getBlacklightSolrUrl() {
    	if (values.containsKey("blacklightSolrUrl")) {
    		return values.get("blacklightSolrUrl");
    	}
		return null;
    }

	public void setTestMode(boolean b) {
		values.put("testMode",(b)?"true":"false");
	}
	public boolean getTestMode() {
		if (values.containsKey("testMode")) {
			return (values.get("testMode").equals("true"))?true:false;
		}
		return false;
	}

	public Integer getEndOfIterativeCatalogUpdates() throws ConfigurationException {
    	final String usage = "Configuration parameter endOfIterativeCatalogUpdates is expected "
    			+ "to be an integer representing the hour to stop processing on a 24-hour clock. "
				+ "For example, to stop processing catalog updates at 6pm, enter the number '18'.";
    	if (values.containsKey("endOfIterativeCatalogUpdates")) {
    		try  {
    			Integer hour = Integer.valueOf(values.get("endOfIterativeCatalogUpdates"));
    			if (hour < 1 || hour > 24)
        			throw new ConfigurationException(usage);
    			return hour;
    		} catch ( NumberFormatException e ) {
    			e.printStackTrace();
    			throw new ConfigurationException(usage);
    		}
    	}
    	return null;
    }

    /**
     * @return the tmpDir on local file system
     */
    public String getTmpDir() {
    	if (values.containsKey("tmpDir")) {
    		return values.get("tmpDir");
    	}
		return null;
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
    	String poolsize = values.get("databasePoolsize"+id);
    	int pool = 1;
    	if (poolsize != null)
    		pool = Integer.valueOf(poolsize);
    	
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
		    	cpds.setJdbcUrl( url );
//		    	cpds.setJdbcUrl( url + "?useUnicode=true&characterEncoding=UTF-8" );
		    	cpds.setUser( user );
		    	cpds.setPassword( pass );
		    	cpds.setMaxStatements(35);
		    	cpds.setTestConnectionOnCheckout(true);
		    	cpds.setTestConnectionOnCheckin(true);
		    	// if we retry every thirty seconds for thirty attempts, we should be
		    	// able to handle 15 minutes of database downtime or network interruption.
		    	cpds.setAcquireRetryAttempts(30);
		    	cpds.setAcquireRetryDelay(30  * 1000); // s * ms/s
		    	cpds.setAcquireIncrement(1);
		    	cpds.setMinPoolSize(1);
		    	cpds.setMaxPoolSize(pool);
		    	cpds.setInitialPoolSize(1);
		    	databases.put(id, cpds);
	    	}
	    	if (debug) System.out.println("Connection pool established. Obtaining and returning connection.");
	    	Connection c = databases.get(id).getConnection();
        	if (driver.contains("mysql")) {
            	try (Statement stmt = c.createStatement()) {
            		stmt.executeUpdate("SET NAMES utf8");
            	}
        	}
	    	return c;
    	}
		Class.forName(driver);
	   
		if (debug) System.out.println("Establishing database connection.");
		Connection c = DriverManager.getConnection(url,user,pass);
		if (debug) System.out.println("database connection established.");
		if (driver.contains("mysql")) {
			try (Statement stmt = c.createStatement()) {
				stmt.executeUpdate("SET NAMES utf8");
			}
		}
		return c;
    }
    public void setDatabasePoolsize(String id, int size) {
    	values.put("databasePoolsize"+id, String.valueOf(size));
    }

    public void closeDatabaseConnectionPools() {
    	for (String dbid : databases.keySet())
    		databases.get(dbid).close();
    }


    public String getDailyReports() throws IOException {
    	if (values.containsKey("dailyReports")) {
    		makeDirIfNeeded(values.get("webdavBaseUrl") + "/" + values.get("dailyReports"));
    		return values.get("dailyReports");
    	}
		return null;
    }
    
    public void setDailyReports(String reports){
        values.put("dailyReports", insertIterationContext(reports));
    }
    
    /* Populate SolrBuildConfig values into Hadoop context mapper
     * 
     */
    public Configuration valuesToHadoopConfig(Configuration hadoopConf) {
    	for (String key : values.keySet())  {
    		hadoopConf.set(key, values.get(key));
    		if (debug)
    			System.out.println("Copying "+key+" to hadoopConfig");
    	}
    	return hadoopConf;
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
        	String valueWDate = insertIterationContext(value);
        	if (debug) if ( ! value.equals(valueWDate)) System.out.println("\t\t==> "+valueWDate);
        	solrBuildConfig.values.put(field,valueWDate);
        }
        if (solrBuildConfig.values.containsKey("debugRSTF")) {
        	String[] debugClasses = solrBuildConfig.values.get("debugRSTF").split(",\\s*");
        	for (String className : debugClasses) {
        		try {
        			solrBuildConfig.setDebugRSTFClass(
        					Class.forName("edu.cornell.library.integration.indexer.solrFieldGen."+className));
				} catch (ClassNotFoundException e) {
					System.out.println("Debug for class "+className+" failed due to ClassNotFoundException.");
					e.printStackTrace();
				}
        	}
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
    	Collection<String> requiredFields = new HashSet<>();
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
    	List<InputStream> inputStreams = new ArrayList<>();
    	for (String arg : argv) {
    		if (arg.endsWith(".properties")) {
    			if (debug) System.out.println("loading from command line arg: "+arg);
    			inputStreams.add(getFile(arg));
    		}
    	}
    	return loadFromPropertiesFile(inputStreams);
    }


    /**
     * Load from the env var. It might be a single file name, or two file names seperated 
     * by a comma.  Also check the classpath.
     */
    private static SolrBuildConfig loadFromEnvVar( String value ) throws Exception {
        System.out.println("loading from environment variable '" + VOYAGER_TO_SOLR_CONFIG + "'="+value);

        String[] names = value.split(",");
        List<InputStream> inputStreams = new ArrayList<>();
        for (String name : names)
        	inputStreams.add(getFile(name));
        return loadFromPropertiesFile( inputStreams );
    }


    /**
     * Load properties for VoyagerToBlacklightSolrConfiguration.
     * If both inA and inB are not null, inA will be loaded as defaults and inB will be
     * loaded as overrides to the values in inA.
     * 
     * If inB is null, only inA will be loaded. 
     */
    public static SolrBuildConfig loadFromPropertiesFile(List<InputStream> inputs) 
            throws IOException{
        
        Properties prop = new Properties();

        for (InputStream in : inputs) {
        	prop.load( in );
        }
        
        SolrBuildConfig conf = new SolrBuildConfig();

        if (debug) System.out.println("Adding all properties to program config.");
        Iterator<String> i = prop.stringPropertyNames().iterator();
        while (i.hasNext()) {
        	String field = i.next();
        	String value = prop.getProperty(field);
        	if (debug) System.out.println(field+" => "+value);
        	String valueWDate = insertIterationContext(value);
        	if (debug) if ( ! value.equals(valueWDate)) System.out.println("\t\t==> "+valueWDate);
        	conf.values.put(field,valueWDate);
        	
        }

        return conf;
    }
    
    /**
     * 
     * @param String value - configuration value. 
     * @return if value contains "XXXX", returns string with "XXXX" replaced with the BUILD_NUMBER
     * environment variable (iff env variable ContextIsBuildNum=true) or current date
     * in YYYY-MM-DD format. If not, returns value unchanged.
     */
    public static String insertIterationContext( String value ) {
    	if (value == null) {
    		// missing config value should be caught later.
    		return null;
    	}
    	if (value.contains("XXXX")) {
    		if (today == null)
    			if (Boolean.valueOf(System.getenv("ContextIsBuildNum")))
    				today = System.getenv("BUILD_NUMBER");
    		if (today == null) {
    			Calendar now = Calendar.getInstance();
    			today = new SimpleDateFormat("yyyy-MM-dd").format(now.getTime());
    		}
    		return value.replace("XXXX", today);
    	}
		return value;
    }
	static String today = null;
 
    /**
     * Returns empty String if configuration is good.
     * Otherwise, it returns a message describing what is missing or problematic.
     */
    public static String checkConfiguration( Collection<String> requiredArgs, SolrBuildConfig checkMe){
        String errMsgs = "";

        // fields required for select processes
        if (requiredArgs.contains("webdavBaseUrl"))
        	errMsgs += checkWebdavUrl( checkMe.values.get("webdavBaseUrl"));
        if (requiredArgs.contains("webdavUser"))
        	errMsgs += checkExists( checkMe.values.get("webdavUser") , "webdavUser");
        if (requiredArgs.contains("webdavPassword"))
        	errMsgs += checkExists( checkMe.values.get("webdavPassword") , "webdavPassword");
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
        if (requiredArgs.contains("batchInfoDir"))
        	errMsgs += checkDir( checkMe.values.get("batchInfoDir"), "batchInfoDir");
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
        }
		InputStream is = SolrBuildConfig.class.getClassLoader().getResourceAsStream(name);
		if( is == null )
		    throw new FileNotFoundException("Could not find file in file system or on classpath: " + name );
		return is;                        
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

