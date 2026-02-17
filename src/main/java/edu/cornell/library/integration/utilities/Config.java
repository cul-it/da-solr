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

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.cornell.library.integration.folio.FolioClient;

/**
 * This is a basic structure intended to hold all the configuration information
 * needed for all steps of the Folio inventory records and convert to Blacklight
 * Solr index.
 * 
 */
public class Config {

	private Map<String, String> values = new HashMap<>();
	private Map<String, ComboPooledDataSource> databases = new HashMap<>();
	private Map<String, FolioClient> folioClients = new HashMap<>();

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

	public String getAuthorityDataDirectory() {
		if (this.values.containsKey("authorityDataDirectory"))
			return this.values.get("authorityDataDirectory");
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

	public String getHathiJobInputPath() {
		if (this.values.containsKey("hathiJobInputPath")) {
			return this.values.get("hathiJobInputPath");
		}
		return null;
	}

	public String getBlacklightUrl() {
		if (this.values.containsKey("blacklightUrl")) {
			return this.values.get("blacklightUrl");
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
		if (this.values.containsKey("solrUrl") &&
				this.values.containsKey("blacklightSolrCore")) {
				return this.values.get("solrUrl")+"/"+this.values.get("blacklightSolrCore");
			}
			return null;
	}

	public String getAuthorSolrUrl() {
		if (this.values.containsKey("solrUrl") &&
				this.values.containsKey("authorSolrCore")) {
				return this.values.get("solrUrl")+"/"+this.values.get("authorSolrCore");
			}
			return null;
	}

	public String getAuthorTitleSolrUrl() {
		if (this.values.containsKey("solrUrl") &&
				this.values.containsKey("authorTitleSolrCore")) {
				return this.values.get("solrUrl")+"/"+this.values.get("authorTitleSolrCore");
			}
			return null;
	}

	public String getSubjectSolrUrl() {
		if (this.values.containsKey("solrUrl") &&
			this.values.containsKey("subjectSolrCore")) {
			return this.values.get("solrUrl")+"/"+this.values.get("subjectSolrCore");
		}
		return null;
	}

	public String getCallnumSolrUrl() {
		if (this.values.containsKey("solrUrl") &&
				this.values.containsKey("callnumSolrCore")) {
				return this.values.get("solrUrl")+"/"+this.values.get("callnumSolrCore");
			}
			return null;
	}

	public String getSolrUser() {
		if (this.values.containsKey("solrUser")) {
			return this.values.get("solrUser");
		}
		return null;
	}

	public String getSolrPassword() {
		if (this.values.containsKey("solrPassword")) {
			return this.values.get("solrPassword");
		}
		return null;
	}

	public String getAnnexFlipsUrl() {
		if (this.values.containsKey("annexFlipsUrl")) {
			return this.values.get("annexFlipsUrl");
		}
		return null;
	}

	public String getAwsS3Bucket() {
		if (this.values.containsKey("awsS3Bucket")) {
			return this.values.get("awsS3Bucket");
		}
		return null;
	}

	public Map<String,String> getServerConfig(String serverPrefix) {
		Map<String,String> args = new HashMap<>();
		for (String key : this.values.keySet())
			if (key.startsWith(serverPrefix))
				args.put(key.replaceFirst(serverPrefix, ""), values.get(key));
		return args;
	}

	public int getRandomGeneratorWavelength() {
		if (this.values.containsKey("randomGeneratorWavelength"))
			return Integer.valueOf(this.values.get("randomGeneratorWavelength"));
		return 400;
	}

	public boolean activateS3() {
		if (! this.values.containsKey("awsS3AccessKey")) return false;
		if (! this.values.containsKey("awsS3SecretKey")) return false;
		Properties props = System.getProperties();
		props.setProperty("aws.accessKeyId", this.values.get("awsS3AccessKey"));
		props.setProperty("aws.secretAccessKey", this.values.get("awsS3SecretKey"));
		return true;
	}

	public boolean activateSES() {
		if (! this.values.containsKey("awsAccessKey")) return false;
		if (! this.values.containsKey("awsSecretKey")) return false;
		Properties props = System.getProperties();
		props.setProperty("aws.accessKeyId", this.values.get("awsAccessKey"));
		props.setProperty("aws.secretAccessKey", this.values.get("awsSecretKey"));
		return true;
	}

	public boolean isFolioConfigured(String id) {
		if ( ! this.values.containsKey("folioUrl"+id) ) return false;
		if ( ! this.values.containsKey("folioTenant"+id) ) return false;
		if ( ! (this.values.containsKey("folioUser"+id) && this.values.containsKey("folioPass"+id) )
				&& ! this.values.containsKey("folioToken"+id) ) return false;

		return true;
	}
	public boolean isTestFolioConfigured(String id) {
		if ( ! this.values.containsKey("folioUrl"+id) ) return false;
		if ( ! this.values.get("folioUrl"+id).contains("test")) return false;
		if ( ! this.values.containsKey("folioTenant"+id) ) return false;
		if ( ! (this.values.containsKey("folioUser"+id) && this.values.containsKey("folioPass"+id) )
				&& ! this.values.containsKey("folioToken"+id) ) return false;

		return true;
	}
	public FolioClient getFolio(String id) throws IOException {
		if (folioClients.containsKey(id))
			return folioClients.get(id);

		FolioClient folio = new FolioClient(
				id,
				this.values.get("folioUrl"+id),
				this.values.get("folioTenant"+id),
				this.values.get("folioUser"+id),
				this.values.get("folioPass"+id));
		folioClients.put(id, folio);
		return folio;
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
	 * use the environment variable CONFIG_FILE_NAME to indicate the property file to use.
	 * 
	 * @param requiredFields is a task-specific Collection of field names required
	 *                       for the task.
	 */
	public static Config loadConfig(Collection<String> requiredFields) {
		String v2bl_config = System.getenv(CONFIG_FILE_NAME);

		if (v2bl_config == null)
			throw new RuntimeException("No configuration specified. \n"
					+ "A configuration is expected in the environment variable "
					+ CONFIG_FILE_NAME + ".\n" + HELP);

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
		System.out.println("loading from environment variable '" + CONFIG_FILE_NAME + "'=" + value);

		String[] names = value.split(",");
		List<InputStream> inputStreams = new ArrayList<>();
		for (String name : names)
			inputStreams.add(getFile(name));
		return loadFromPropertiesFile(inputStreams);
	}

	/**
	 * Load properties for the job configuration. If both inA and inB
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
	private static String checkConfiguration(Collection<String> requiredArgs, Config configToCheck) {
		String errMsgs = "";

		for (String arg : requiredArgs)
			if (arg.endsWith("SolrUrl"))
				errMsgs += checkSolrUrl(arg, configToCheck);
			else if (arg.endsWith("Url") || arg.endsWith("UriPrefix"))
				errMsgs += checkUrl(arg, configToCheck.values.get(arg));
			else
				errMsgs += checkExists(arg, configToCheck.values.get(arg));

		return errMsgs;
	}

	private static String checkExists(String propName, String value) {
		if (value == null || value.trim().isEmpty())
			return "The property " + propName + " must not be empty or null.\n";
		return "";
	}

	private static String checkSolrUrl(String propName, Config configToCheck) {
		String errMsgs = "";
		if (! configToCheck.values.containsKey("solrUrl") ) 
			errMsgs += "solrUrl is a required parameter in suppled config for this task.";
		if (! configToCheck.values.containsKey("solrUser") ) 
			errMsgs += "solrUser is a required parameter in suppled config for this task.";
		if (! configToCheck.values.containsKey("solrPassword") ) 
			errMsgs += "solrPassword is a required parameter in suppled config for this task.";
		if (propName.equals("blacklightSolrUrl")) {
			if (! configToCheck.values.containsKey("blacklightSolrCore"))
				errMsgs += "blacklightSolrCore is a required parameter in suppled config for this task.";
		} else if (propName.equals("authorSolrUrl")) {
			if (! configToCheck.values.containsKey("authorSolrCore"))
				errMsgs += "authorSolrCore is a required parameter in suppled config for this task.";
		} else if (propName.equals("authorTitleSolrUrl")) {
			if (! configToCheck.values.containsKey("authorTitleSolrCore"))
				errMsgs += "authorTitleSolrCore is a required parameter in suppled config for this task.";
		} else if (propName.equals("subjectSolrUrl")) {
			if (! configToCheck.values.containsKey("subjectSolrCore"))
				errMsgs += "subjectSolrCore is a required parameter in suppled config for this task.";
		} else if (propName.equals("callnumSolrUrl")) {
			if (! configToCheck.values.containsKey("callnumSolrCore"))
				errMsgs += "callnumSolrCore is a required parameter in suppled config for this task.";
		} else {
			errMsgs += propName;
			errMsgs += " is not a valid Solr Url.";
		}
			
		return errMsgs;
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
	private final static String CONFIG_FILE_NAME = "configFile";

	private final static String HELP = "The environment variable "+CONFIG_FILE_NAME+" must be set to one"
			+ " or more properties files:\n"
			+ "Ex. "+CONFIG_FILE_NAME+"=prod.properties,database.properties java someClass\n"
			+ "These files will be searched for first in the file system, then from the classpath/ClassLoader.\n";
}
