package edu.cornell.library.integration.db_test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class SqliteBaseTest {
	protected static final String DBNAME = "test";
	protected static final String DBUID = "test_user";
	protected static final String DBPWD = "test_pwd";
	protected static final String INIT_DB_PATH = Path.of("src", "test", "resources", "example_db_data", "base.db").toString();
	protected static final String TEST_DB_PATH = Path.of("src", "test", "resources", "example_db_data", "copy.db").toString();
	protected static Properties PROPS = null;
	protected static File sourceInitDb = null;
	protected static File testPropertiesFile = null;

	public static Properties setProperties() throws FileNotFoundException, IOException {
		File sqliteDb = createSqliteData();

		if (PROPS != null) {
			return PROPS;
		}

		PROPS = new Properties();
		String jdbcPath = "jdbc:sqlite:" + sqliteDb.getAbsolutePath();
		for (String id : Arrays.asList("CallNos", "Current", "Hathi", "Headings")) {
			PROPS.setProperty("databaseURL" + id, jdbcPath);
			PROPS.setProperty("databaseUser" + id, DBUID);
			PROPS.setProperty("databasePass" + id, DBPWD);
			PROPS.setProperty("databasePooling" + id, "false");
		}
		PROPS.setProperty("catalogClass", "edu.cornell.library.integration.folio");

		testPropertiesFile = new File(DbBaseTest.TEST_PROPERTIES_PATH);
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(DbBaseTest.TEST_PROPERTIES_PATH))) {
			PROPS.store(os, null);
		}
		testPropertiesFile.deleteOnExit();

		return PROPS;
	}

	public static void init(List<String> sqls) throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
		if (sourceInitDb == null) {
			sourceInitDb = new File(INIT_DB_PATH);
			sourceInitDb.deleteOnExit();
			Connection conn = DriverManager.getConnection("jdbc:sqlite:"+sourceInitDb.getAbsolutePath());
			for (String sql : sqls) {
				try (Statement stmt = conn.createStatement();
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sql),"UTF-8"))) {
					String line;
					while ((line = br.readLine()) != null) {
						if (StringUtils.isBlank(line)) {
							continue;
						}
						stmt.executeUpdate(line);
					}
				}
			}
			conn.close();
		}
	}

	/*
	 * Return a fresh copy of INIT_DB_PATH so each test will have a clean slate.
	 */
	protected static File createSqliteData() throws IOException {
		File db = new File(TEST_DB_PATH);
		FileUtils.copyFile(sourceInitDb, db);
		db.deleteOnExit();

		return db;
	}
}
