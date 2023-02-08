package edu.cornell.library.integration.db_test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;

import org.testcontainers.containers.MySQLContainer;

public class AbstractContainerBaseTest {
	protected static final String DBDRIVER = "com.mysql.jdbc.Driver";
	protected static final String DBNAME = "test";
	protected static final String DBUID = "test_user";
	protected static final String DBPWD = "test_pwd";
	protected static Properties PROPS = null;
	@SuppressWarnings("rawtypes")
	protected final static MySQLContainer mysqlContainer;
	protected static File testPropertiesFile = null;

	static {
		mysqlContainer = new MySQLContainer<>("mysql:latest")
				.withDatabaseName(DBNAME)
				.withUsername(DBUID)
				.withPassword(DBPWD);
		mysqlContainer.withInitScript("example_db_data/db_initialization.sql");
		mysqlContainer.start();
	}

	public static Properties setProperties() throws FileNotFoundException, IOException {
		if (PROPS != null) {
			return PROPS;
		}

		PROPS = new Properties();
		for (String id : Arrays.asList("CallNos", "Current", "Hathi", "Headings")) {
			PROPS.setProperty("databaseDriver" + id, DBDRIVER);
			PROPS.setProperty("databaseURL" + id, mysqlContainer.getJdbcUrl());
			PROPS.setProperty("databaseUser" + id, DBUID);
			PROPS.setProperty("databasePass" + id, DBPWD);
			PROPS.setProperty("databasePoolsize" + id, "2");
			PROPS.setProperty("databasePooling" + id, "false");
		}
		PROPS.setProperty("catalogClass", "edu.cornell.library.integration.folio");
		
		testPropertiesFile = new File("src/test/resources/example_db_data/folio-unit-test.properties");
		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(testPropertiesFile))) {
			PROPS.store(os, null);
		}
		
		return PROPS;
	}

//	public void clearTestPropertiesFile() throws IOException {
//		try (FileWriter fw = new FileWriter(new File(testPropertiesFile.getAbsolutePath()), false)) {
//			fw.flush();
//		}
//	}
}
