package edu.cornell.library.integration.db_test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.platform.commons.util.StringUtils;

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
 * - If UseTestContainers and UseSqlite are not set, it will use the configFile to connect to DB directly.
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

	public static void setup(String dbName) throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB(dbName);
		setup(dbName, requiredArgs);
	}

	public static void setup(String dbName, List<String> requiredArgs) throws IOException, SQLException {
		useTestContainers = System.getenv(USE_TEST_CONTAINERS);
		useSqlite = System.getenv(USE_SQLITE);

		if (useTestContainers != null) {
			AbstractContainerBaseTest.init(getInitSqls(MYSQL_CREATE_STATEMENTS_PATH));
			AbstractContainerBaseTest.setProperties();
			config = Config.loadConfig(requiredArgs, TEST_PROPERTIES_PATH);
		} else if (useSqlite != null) {
			SqliteBaseTest.init(getInitSqls(SQLITE_CREATE_STATEMENTS_PATH));
			SqliteBaseTest.setProperties();
			config = Config.loadConfig(requiredArgs, TEST_PROPERTIES_PATH);
		} else {
			config = Config.loadConfig(requiredArgs);
		}
	}

	public static void runStmt(List<String> sqls, Connection conn) throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
		for (String sql : sqls) {
			try (Statement stmt = conn.createStatement();
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sql),"UTF-8"))) {
				String line;
				while ((line = br.readLine()) != null) {
					if (StringUtils.isBlank(line) || line.startsWith("--")) continue;
					stmt.executeUpdate(line);
				}
			}
		}
	}

	public static void executeStmt(List<String> sqls, Connection conn) throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
		for (String sql : sqls) {
			try (Statement stmt = conn.createStatement()) {
				if (StringUtils.isBlank(sql) || sql.startsWith("--")) continue;
				// madsAuthorityUpdate table has source column that is of type JSON in mysql (mariadb) and text in sqlite.
				// When inserting with prepared statemements with parameters, everything works fine.
				// When we insert using plain sql statements in tests, we need to handle the differences in how MySQL and SQLite treat JSON and text columns.
				if (useSqlite != null)
					sql = sql.replace("\\\\", "\\");
				stmt.executeUpdate(sql);
			}
		}
	}

	protected static List<String> getInitSqls(String createPath) {
		List<String> sqls = new ArrayList<>();
		sqls.add(createPath);
		sqls.add(DB_INIT_PATH);
		return sqls;
	}
}
