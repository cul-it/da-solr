package edu.cornell.library.integration.db_test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import edu.cornell.library.integration.utilities.Config;

/*
 * Requirements to run these tests with local MySQL:
 * 1. Docker must be installed on the machine running these tests.
 * 2. UseTestContainers env var needs to be set.
 * 
 * UseTestContainers=true
 * 
 * - As long as UseTestContainers variable is set with any value, DB container will start.
 * 
 * To run tests using SQLITE, set the following env var:
 * 
 * UseSqlite=true
 * 
 * - If UseTestContainers and UseSqlite are not set, it will use the VoyagerToSolrConfig to connect to DB directly.
 */
public class DbBaseTest {
	protected static Config config = null;
	protected static final String USE_TEST_CONTAINERS = "UseTestContainers";
	protected static final String USE_SQLITE = "UseSqlite";
	protected static String useTestContainers = null;
	protected static String useSqlite = null;
	protected static final String TEST_RESOURCE_PATH = Path.of("src", "test", "resources", "example_db_data").toString();
	public static final String TEST_PROPERTIES_PATH = new File(TEST_RESOURCE_PATH, "test.properties").getAbsolutePath();
	protected static final String MYSQL_CREATE_STATEMENTS_PATH = new File(TEST_RESOURCE_PATH, "mysql_create_statements.sql").getAbsolutePath();
	protected static final String SQLITE_CREATE_STATEMENTS_PATH = new File(TEST_RESOURCE_PATH, "sqlite_create_statements.sql").getAbsolutePath();
	protected static final String DB_INIT_PATH = new File(TEST_RESOURCE_PATH, "db_initialization.sql").getAbsolutePath();
	protected static final String DEST_SQL_PATH = new File(TEST_RESOURCE_PATH, "initialization.sql").getAbsolutePath();
	protected static File mysqlInitFile = null;
	protected static File sqliteInitFile = null;

	public static void setup(String dbName) throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB(dbName);
		setup(dbName, requiredArgs);
	}

	public static void setup(String dbName, List<String> requiredArgs) throws IOException, SQLException {
		useTestContainers = System.getenv(USE_TEST_CONTAINERS);
		useSqlite = System.getenv(USE_SQLITE);

		if (useTestContainers != null) {
			if (mysqlInitFile == null) {
				mysqlInitFile = getlInitSql(MYSQL_CREATE_STATEMENTS_PATH);
			}
			AbstractContainerBaseTest.init(mysqlInitFile);
			AbstractContainerBaseTest.setProperties();
			mysqlInitFile.deleteOnExit();
			config = Config.loadConfig(requiredArgs, TEST_PROPERTIES_PATH);
		} else if (useSqlite != null) {
			if (sqliteInitFile == null) {
				sqliteInitFile = getlInitSql(SQLITE_CREATE_STATEMENTS_PATH);
			}
			SqliteBaseTest.init(sqliteInitFile);
			SqliteBaseTest.setProperties();
			sqliteInitFile.deleteOnExit();
			config = Config.loadConfig(requiredArgs, TEST_PROPERTIES_PATH);
		} else {
			config = Config.loadConfig(requiredArgs);
		}
	}

	// https://stackoverflow.com/questions/14673063/merging-multiple-files-in-java
	protected static void joinFiles(File destination, List<File> sources) throws IOException {
		OutputStream output = null;
		try {
			output = createAppendableStream(destination);
			for (File source : sources) {
				appendFile(output, source);
			}
		} finally {
			IOUtils.closeQuietly(output);
		}
	}

	protected static BufferedOutputStream createAppendableStream(File destination) throws FileNotFoundException {
		return new BufferedOutputStream(new FileOutputStream(destination, true));
	}

	protected static void appendFile(OutputStream output, File source)
			throws IOException {
		InputStream input = null;
		try {
			input = new BufferedInputStream(new FileInputStream(source));
			IOUtils.copy(input, output);
		} finally {
			IOUtils.closeQuietly(input);
		}
	}

	public static File getlInitSql(String createPath) throws IOException {
		File initSql = new File(DEST_SQL_PATH);
		initSql.deleteOnExit();
		List<File> sources = new ArrayList<File>();
		sources.add(new File(createPath));
		sources.add(new File(DB_INIT_PATH));
		joinFiles(initSql, sources);

		return initSql;
	}
}
