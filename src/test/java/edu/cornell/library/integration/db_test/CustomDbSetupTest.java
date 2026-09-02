package edu.cornell.library.integration.db_test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.utilities.Config;

public class CustomDbSetupTest extends DbBaseTest {
	protected static String sqliteExtraSql = null;
	protected static List<Function<Connection, Void>> testContainersSetupFunctions = new ArrayList<>();
	protected static List<String> extraInsertSqls = new ArrayList<>();

	public static void custom_setup() throws IOException, SQLException, InterruptedException, JsonLdError, URISyntaxException {
		System.out.println("custom setup running");
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Authority"));
		setup("Headings", requiredArgs);
		// test db puts everything into single test db, it isn't important whether it's Headings or Authority
		try ( Connection conn = config.getDatabaseConnection("Headings")) {
			if (useTestContainers != null) {
				createTablesForTestContainers(conn);
			} else if (useSqlite != null) {
				// String extraSqlite = Path.of(base, "authority_extra_sqlite_create.sql").toString();
				String sql = new File(sqliteExtraSql).getAbsolutePath();
				DbBaseTest.runStmt(Arrays.asList(sql), conn);
			}
			for (String extraSql : extraInsertSqls)
				DbBaseTest.executeStmt(Arrays.asList(extraSql), conn);
		}
	}

	public static void createTablesForTestContainers(Connection conn) throws SQLException {
		for (var f : testContainersSetupFunctions)
			f.apply(conn);
	}
}
