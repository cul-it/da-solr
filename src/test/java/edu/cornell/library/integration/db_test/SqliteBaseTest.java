package edu.cornell.library.integration.db_test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Properties;

import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

public class SqliteBaseTest {
	protected static final String DBDRIVER = "com.sqlite.jdbc.Driver";
	protected static final String DBNAME = "test";
	protected static final String DBUID = "test_user";
	protected static final String DBPWD = "test_pwd";
	protected static final String INIT_DB_PATH = "src/test/resources/example_db_data/test";
	protected static final String TEST_DB_PATH = "src/test/resources/example_db_data/test.db";
	protected static Properties PROPS = null;
	protected static File sourceInitDb = null;

	public static Properties setProperties() throws FileNotFoundException, IOException {
		File sqliteDb = createSqliteData();

		if (PROPS != null) {
			return PROPS;
		}

		PROPS = new Properties();
		String jdbcPath = "jdbc:sqlite:" + sqliteDb.getAbsolutePath();
		for (String id : Arrays.asList("CallNos", "Current", "Hathi", "Headings")) {
			PROPS.setProperty("databaseDriver" + id, DBDRIVER);
			PROPS.setProperty("databaseURL" + id, jdbcPath);
			PROPS.setProperty("databaseUser" + id, DBUID);
			PROPS.setProperty("databasePass" + id, DBPWD);
			PROPS.setProperty("databasePoolsize" + id, "2");
			PROPS.setProperty("databasePooling" + id, "false");
		}
		PROPS.setProperty("catalogClass", "edu.cornell.library.integration.folio");

		try (OutputStream os = new BufferedOutputStream(new FileOutputStream(DbBaseTest.TEST_PROPERTIES_PATH))) {
			PROPS.store(os, null);
		}

		return PROPS;
	}

	/*
	 * Run "sqlite3 DBNAME < INIT_DB_PATH" to generate sqlite3 db file named DBNAME at the project root directory.
	 * Move the generated sqlite3 db file to INIT_DB_PATH.
	 * The generated db file will be deleted after all the DB tests are run.
	 */
	public static void init(File sqliteSource) {
		try {
			ProcessBuilder builder = new ProcessBuilder("sqlite3", DBNAME);
			builder.redirectInput(sqliteSource);
			Process p = builder.start();
			// should we set maximum wait time?
			try {
				p.waitFor();
			} catch (InterruptedException e) {}
			File src = new File(DBNAME);
			sourceInitDb = new File(INIT_DB_PATH);
			sourceInitDb.deleteOnExit();
			Files.move(Paths.get(src.getAbsolutePath()), Paths.get(sourceInitDb.getAbsolutePath()), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException ioe) {
			System.out.println("Failed to initialize Sqlite DB: " + ioe.toString());
			System.exit(1);
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
