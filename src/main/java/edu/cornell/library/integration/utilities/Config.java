package edu.cornell.library.integration.utilities;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.ConfigurationException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.cornell.library.integration.folio.OkapiClient;

/**
 * This is a basic structure intended to hold all the configuration information
 * needed for all steps of the Voyager MARC21 extract and convert to Blacklight
 * Solr index.
 * 
 * The goal is to facilitate the creation of various configurations that can be
 * called for from the command line of the different steps of the conversion.
 * 
 * See the method loadConfig(String[]) for how the SolrBuildConf can be loaded.
 * 
 * If you'd like to add a new configuration setting to this class, 1 add a
 * property to this class 2 add a getter to this class 3 make sure your property
 * is loaded in loadFromPropertiesFile 4 make sure your property is checked in
 * checkConfiguration 5 add your property to the example properties file at
 * voyagerToSolrConfig.properties.example
 */
public class Config {

	private Map<String, String> values = new HashMap<>();
	private Map<String, ComboPooledDataSource> databases = new HashMap<>();

	public static List<String> getRequiredArgsForDB(String db) {
		List<String> list = new ArrayList<>();
		if (db == null)
			return null;
		if (db.isEmpty())
			return null;
		list.add("databaseURL" + db);
		list.add("databaseUser" + db);
		list.add("databasePass" + db);
		return list;
	}

	public String getCatalogClass() {
		if (this.values.containsKey("catalogClass"))
			return this.values.get("catalogClass");
		return null;
	}
	public void setCatalogClass(String catalogClass) {
		this.values.put("catalogClass",catalogClass);
	}

	public Boolean isProduction() {
		if (this.values.containsKey("production") && this.values.get("production").equals("true"))
			return true;
		return false;
	}

	public String getHathiUpdatesFilesDirectory() {
		if (this.values.containsKey("hathiUpdatesFilesDirectory"))
			return this.values.get("hathiUpdatesFilesDirectory");
		return null;
	}

	public String getAuthorityChangeFileDirectory() {
		if (this.values.containsKey("authorityChangeFileDirectory"))
			return this.values.get("authorityChangeFileDirectory");
		return null;
	}

	public String getHathifilesUrl() {
		if (this.values.containsKey("hathifilesUrl")) {
			return this.values.get("hathifilesUrl");
		}
		return null;
	}


	public String getSolrUrl() {
		if (this.values.containsKey("solrUrl")) {
			return this.values.get("solrUrl");
		}
		return null;
	}

	public String getBlacklightSolrUrl() {
		if (this.values.containsKey("blacklightSolrUrl")) {
			return this.values.get("blacklightSolrUrl");
		}
		return null;
	}

	public String getAuthorSolrUrl() {
		if (this.values.containsKey("authorSolrUrl")) {
			return this.values.get("authorSolrUrl");
		}
		return null;
	}

	public String getAuthorTitleSolrUrl() {
		if (this.values.containsKey("authorTitleSolrUrl")) {
			return this.values.get("authorTitleSolrUrl");
		}
		return null;
	}

	public String getSubjectSolrUrl() {
		if (this.values.containsKey("subjectSolrUrl")) {
			return this.values.get("subjectSolrUrl");
		}
		return null;
	}

	public String getCallnumSolrUrl() {
		if (this.values.containsKey("callnumSolrUrl")) {
			return this.values.get("callnumSolrUrl");
		}
		return null;
	}

	public String getAnnexFlipsUrl() {
		if (this.values.containsKey("annexFlipsUrl")) {
			return this.values.get("annexFlipsUrl");
		}
		return null;
	}

	public Integer getEndOfIterativeCatalogUpdates() throws ConfigurationException {
		final String usage = "Configuration parameter endOfIterativeCatalogUpdates is expected "
				+ "to be an integer representing the hour to stop processing on a 24-hour clock. "
				+ "For example, to stop processing catalog updates at 6pm, enter the number '18'.";
		if (this.values.containsKey("endOfIterativeCatalogUpdates")) {
			try {
				Integer hour = Integer.valueOf(this.values.get("endOfIterativeCatalogUpdates"));
				if (hour < 1 || hour > 24)
					throw new ConfigurationException(usage);
				return hour;
			} catch (NumberFormatException e) {
				e.printStackTrace();
				throw new ConfigurationException(usage);
			}
		}
		return null;
	}

	public int getRandomGeneratorWavelength() {
		if (this.values.containsKey("randomGeneratorWavelength"))
			return Integer.valueOf(this.values.get("randomGeneratorWavelength"));
		return 400;
	}

	public boolean isOkapiConfigured(String id) {
		if ( ! this.values.containsKey("okapiUrl"+id) ) return false;
		if ( ! this.values.containsKey("okapiTenant"+id) ) return false;
		if ( ! (this.values.containsKey("okapiUser"+id) && this.values.containsKey("okapiPass"+id) )
				&& ! this.values.containsKey("okapiToken"+id) ) return false;

		return true;
	}
	public OkapiClient getOkapi(String id) throws IOException {
		OkapiClient okapi = new OkapiClient(
				this.values.get("okapiUrl"+id),
				this.values.get("okapiTenant"+id),
				this.values.get("okapiToken"+id),
				this.values.get("okapiUser"+id),
				this.values.get("okapiPass"+id));
		if ( ! this.values.containsKey("okapiToken"+id) ) {
			this.values.put("okapiToken"+id, okapi.getToken());
			System.out.println("Recording token to config: "+okapi.getToken());
		}
		return okapi;
	}

	public boolean isDatabaseConfigured(String id) {
		if ( ! this.values.containsKey("databaseURL"+id) ) return false;
		if ( ! this.values.containsKey("databaseUser"+id) ) return false;
		if ( ! this.values.containsKey("databasePass"+id) ) return false;
		return true;
	}
	/**
	 * @param id : database identifier used in config properties file
	 * @return java.sql.Connection
	 * @throws SQLException
	 */
	public Connection getDatabaseConnection(String id) throws SQLException {
		String url = this.values.get("databaseURL" + id);
		if (url == null) {
			System.out.println("Value not found for databaseURL" + id);
			System.exit(1);
		}
		String user = this.values.get("databaseUser" + id);
		if (user == null) {
			System.out.println("Value not found for databaseUser" + id);
			System.exit(1);
		}
		String pass = this.values.get("databasePass" + id);
		if (pass == null) {
			System.out.println("Value not found for databasePass" + id);
			System.exit(1);
		}
		String maxStatementStr = this.values.get("databaseMaxStatements" + id);
		Integer maxStatements = (maxStatementStr == null) ? 35 : Integer.valueOf(maxStatementStr);
		String poolsize = this.values.get("databasePoolsize" + id);
		int pool = 1;
		if (poolsize != null)
			pool = Integer.valueOf(poolsize);

		Boolean pooling = true; // default if not specified in config
		if (this.values.containsKey("databasePooling" + id))
			pooling = Boolean.valueOf(this.values.get("databasePooling" + id));

		if (pooling) {
			if (!this.databases.containsKey(id)) {
				ComboPooledDataSource cpds = new ComboPooledDataSource();
				cpds.setJdbcUrl(url);
//		    	cpds.setJdbcUrl( url + "?useUnicode=true&characterEncoding=UTF-8" );
				cpds.setUser(user);
				cpds.setPassword(pass);
				cpds.setMaxStatements(maxStatements);
				cpds.setTestConnectionOnCheckout(true);
				cpds.setTestConnectionOnCheckin(true);
				// if we retry every thirty seconds for thirty attempts, we should be
				// able to handle 15 minutes of database downtime or network interruption.
				cpds.setAcquireRetryAttempts(30);
				cpds.setAcquireRetryDelay(30 * 1000); // s * ms/s
				cpds.setAcquireIncrement(1);
				cpds.setMinPoolSize(1);
				cpds.setMaxPoolSize(pool);
				cpds.setInitialPoolSize(1);
				this.databases.put(id, cpds);
			}
			Connection c = this.databases.get(id).getConnection();
			if (url != null && url.contains("mysql")) {
				try (Statement stmt = c.createStatement()) {
					stmt.executeUpdate("SET NAMES utf8");
				}
			}
			return c;
		}

		Connection c = DriverManager.getConnection(url, user, pass);
		if (url != null && url.contains("mysql")) {
			try (Statement stmt = c.createStatement()) {
				stmt.executeUpdate("SET NAMES utf8");
			}
		}
		return c;
	}

	public void setDatabasePoolsize(String id, int size) {
		this.values.put("databasePoolsize" + id, String.valueOf(size));
	}

	/**
	 * A utility method to load properties from command line or environment.
	 * 
	 * Configured jobs on integration servers or automation systems are expected to
	 * use the environment variable VOYAGER_TO_SOLR_CONFIG to indicate the property
	 * file to use.
	 * 
	 * Development jobs are expected to use command line arguments.
	 * 
	 * If properties files exist on the command line use those, If the environment
	 * variable VOYAGER_TO_SOLR_CONFIG exists use those, If both environment
	 * variable VOYAGER_TO_SOLR_CONFIG and command line arguments exist, throw an
	 * error because that is a confused state and likely a problem.
	 * 
	 * The value of the environment variable VOYAGER_TO_SOLR_CONFIG may be a comma
	 * separated list of files.
	 * @param requiredFields is a task-specific Collection of field names required
	 *                       for the task. argv may be null to force use of the
	 *                       environment variable. Should be argv from main().
	 */
	public static Config loadConfig(Collection<String> requiredFields) {
		String v2bl_config = System.getenv(VOYAGER_TO_SOLR_CONFIG);

		if (v2bl_config == null)
			throw new RuntimeException("No configuration specified. \n"
					+ "A configuration is expected in the environment variable "
					+ VOYAGER_TO_SOLR_CONFIG + ".\n" + HELP);

		Config config = null;
		try {
			config = loadFromEnvVar(v2bl_config);
		} catch (Exception ex) {
			throw new RuntimeException("There were problems loading the configuration.\n ", ex);
		}

		return _loadConfig(requiredFields, config);
	}

	/*
	 * A utility method to load properties from config file path.
	 */
	public static Config loadConfig(Collection<String> requiredFields, String configPath) throws IOException {
		Config config = null;

		List<InputStream> inputStreams = new ArrayList<>();
		try {
			inputStreams.add(getFile(configPath));
			config = loadFromPropertiesFile(inputStreams);
		} catch (Exception ex) {
			throw new RuntimeException("There were problems loading the configuration.\n ", ex);
		}

		return _loadConfig(requiredFields, config);
	}

	private static Config _loadConfig(Collection<String> requiredFields, Config config) {
		String errs = checkConfiguration(requiredFields, config);
		if (errs == null || !errs.trim().isEmpty()) {
			throw new RuntimeException("There were problems with the configuration.\n " + errs);
		}

		return config;
	}

	/**
	 * Load from the env var. It might be a single file name, or two file names
	 * seperated by a comma. Also check the classpath.
	 */
	private static Config loadFromEnvVar(String value) throws Exception {
		System.out.println("loading from environment variable '" + VOYAGER_TO_SOLR_CONFIG + "'=" + value);

		String[] names = value.split(",");
		List<InputStream> inputStreams = new ArrayList<>();
		for (String name : names)
			inputStreams.add(getFile(name));
		return loadFromPropertiesFile(inputStreams);
	}

	/**
	 * Load properties for VoyagerToBlacklightSolrConfiguration. If both inA and inB
	 * are not null, inA will be loaded as defaults and inB will be loaded as
	 * overrides to the values in inA.
	 * 
	 * If inB is null, only inA will be loaded.
	 */
	private static Config loadFromPropertiesFile(List<InputStream> inputs) throws IOException {

		Properties prop = new Properties();

		for (InputStream in : inputs) {
			prop.load(in);
		}

		Config conf = new Config();

		Iterator<String> i = prop.stringPropertyNames().iterator();
		while (i.hasNext()) {
			String field = i.next();
			String value = prop.getProperty(field);
			String valueWDate = insertIterationContext(value);
			conf.values.put(field, valueWDate);

		}

		return conf;
	}

	/**
	 * 
	 * @param value - configuration value.
	 * @return if value contains "XXXX", returns string with "XXXX" replaced with
	 *         the BUILD_NUMBER environment variable (iff env variable
	 *         ContextIsBuildNum=true) or current date in YYYY-MM-DD format. If not,
	 *         returns value unchanged.
	 */
	public static String insertIterationContext(String value) {
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

	private static String today = null;

	/**
	 * Returns empty String if configuration is good. Otherwise, it returns a
	 * message describing what is missing or problematic.
	 */
	private static String checkConfiguration(Collection<String> requiredArgs, Config checkMe) {
		String errMsgs = "";

		for (String arg : requiredArgs)
			if (arg.endsWith("Url") || arg.endsWith("UriPrefix"))
				errMsgs += checkUrl(arg, checkMe.values.get(arg));
			else
				errMsgs += checkExists(arg, checkMe.values.get(arg));

		return errMsgs;
	}

	private static String checkExists(String propName, String value) {
		if (value == null || value.trim().isEmpty())
			return "The property " + propName + " must not be empty or null.\n";
		return "";
	}

	private static String checkUrl(String propName, String value) {
		if (value == null || value.trim().isEmpty())
			return "The property field " + propName + " must be set.\n";
		else if (!value.startsWith("http://")&&!value.startsWith("https://"))
			return "The field " + propName + " was '" + value + "' but it must be a URL.\n";
		else
			return "";
	}

	private static InputStream getFile(String name) throws FileNotFoundException {
		File f = new File(name);
		if (f.exists()) {
			return new FileInputStream(f);
		}
		InputStream is = Config.class.getClassLoader().getResourceAsStream(name);
		if (is == null)
			throw new FileNotFoundException("Could not find file in file system or on classpath: " + name);
		return is;
	}

	/**
	 * Name of environment variable for configuration files.
	 */
	private final static String VOYAGER_TO_SOLR_CONFIG = "VoyagerToSolrConfig";

	private final static String HELP = "The environment variable VoyagerToSolrConfig must be set to one"
			+ " or more properties files:\n"
			+ "Ex. VoyagerToSolrConfig=prod.properties,database.properties java someClass\n"
			+ "These files will be searched for first in the file system, then from the classpath/ClassLoader.\n";
}
