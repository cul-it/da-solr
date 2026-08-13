package edu.cornell.library.integration.db_test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.utilities.Config;

public class CustomDbSetupTest extends DbBaseTest {
	protected static String sqliteExtraSql = null;

	public static void custom_setup() throws IOException, SQLException, InterruptedException, JsonLdError, URISyntaxException {
		System.out.println("custom setup running");
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Authority"));
		setup("Headings", requiredArgs);
		try ( Connection conn = config.getDatabaseConnection("Headings")) {
			populateStaticData(conn);
			if (useTestContainers != null) {
				createTablesForTestContainers(conn);
			} else if (useSqlite != null) {
				// String extraSqlite = Path.of(base, "authority_extra_sqlite_create.sql").toString();
				String sql = new File(sqliteExtraSql).getAbsolutePath();
				SqliteBaseTest.runStmt(Arrays.asList(sql), conn);
			}
		}
	}

	public static void populateStaticData(Connection conn) throws SQLException {
		// override this method in subclasses to populate static data in the database when applicable
	}

	public static void createTablesForTestContainers(Connection conn) {
		// override this method in subclasses to create tables in the database when applicable
	}
}
