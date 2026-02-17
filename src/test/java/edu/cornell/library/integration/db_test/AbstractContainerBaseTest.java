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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.platform.commons.util.StringUtils;
import org.testcontainers.containers.MySQLContainer;

public class AbstractContainerBaseTest {
	protected static final String DBNAME = "test";
	protected static final String DBUID = "test_user";
	protected static final String DBPWD = "test_pwd";
	protected static Properties PROPS = null;
	protected final static MySQLContainer<?> mysqlContainer;
	protected static File testPropertiesFile = null;
	protected static boolean initialized = false;

	static {
		mysqlContainer = new MySQLContainer<>("mysql:9.2.0")
				.withDatabaseName(DBNAME)
				.withUsername(DBUID)
				.withPassword(DBPWD)
				.withDatabaseName(DBNAME);
		mysqlContainer.start();
	}

	public static void init(List<String> sqls) throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
		if (!initialized) {
			String jdbc_str = mysqlContainer.getJdbcUrl() + "?user=" + DBUID + "&password=" + DBPWD;
			Connection conn = DriverManager.getConnection(jdbc_str);
			for (String sql : sqls) {
				try (Statement stmt = conn.createStatement();
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sql),"UTF-8"))) {
					String line;
					while ((line = br.readLine()) != null) {
						if (StringUtils.isBlank(line) || line.startsWith("--")) {
							continue;
						}
						stmt.executeUpdate(line);
					}
				}
			}
			conn.close();
			initialized = true;
		}
	}

	public static Properties setProperties() throws FileNotFoundException, IOException {
		if (PROPS != null) {
			return PROPS;
		}

		PROPS = new Properties();
		for (String id : Arrays.asList("Authority", "CallNos", "Current", "Hathi", "Headings")) {
			PROPS.setProperty("databaseURL" + id, mysqlContainer.getJdbcUrl());
			PROPS.setProperty("databaseUser" + id, DBUID);
			PROPS.setProperty("databasePass" + id, DBPWD);
			PROPS.setProperty("databasePooling" + id, "false");
		}
		PROPS.setProperty("catalogClass", "edu.cornell.library.integration.folio");

		testPropertiesFile = new File(DbBaseTest.TEST_PROPERTIES_PATH);
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(testPropertiesFile))) {
			PROPS.store(os, null);
		}
		testPropertiesFile.deleteOnExit();

		return PROPS;
	}
}
