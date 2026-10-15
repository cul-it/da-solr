package edu.cornell.library.integration.authority.activitystreams;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.db_test.DbBaseTest;

public class BDHIntegrationTest extends DbBaseTest {
	static Map<String, Path> resources = new LinkedHashMap<>();

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		setup("Authority");
		String base = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority").toString();
		if (useTestContainers != null) {
			try (Connection authority = config.getDatabaseConnection("Authority")) {
				Utils.setUpDatabase(authority);
			}
		} else if (useSqlite != null) {
			String extraSqlite = Path.of(base, "authority_extra_sqlite_create.sql").toString();
			String sql = new File(extraSqlite).getAbsolutePath();
			try (Connection authority = config.getDatabaseConnection("Authority")) {
				DbBaseTest.runStmt(Arrays.asList(sql), authority);
			}
		}
	}

	@Test
	public void loadBulkJsonLDTest() throws IOException, InterruptedException, JsonLdError, SQLException, URISyntaxException {
		BulkDownloadHandler handler = new BulkDownloadHandler();
		Map<String, String> env = System.getenv();
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			int chunkSize = Integer.parseInt(env.getOrDefault("ChunkSize", "100"));
			boolean deleteOldFile = Boolean.getBoolean(env.getOrDefault("DeleteOldFile", "false"));
			String destinationDir = env.get("DestinationDir");
			boolean initDB = Boolean.getBoolean(env.get("initDB"));
			String jsonldURL = env.get("BulkDownloadURL");
			Path destination = Paths.get(destinationDir, Utils.getDestName(jsonldURL));
//			Path destination = Paths.get(destinationDir, "erroneous.madsrdf.jsonld");
			String today = Utils.getToday();
			handler.run(today, authority, chunkSize, deleteOldFile, destination, initDB, jsonldURL, 0);
		}
	}
}
