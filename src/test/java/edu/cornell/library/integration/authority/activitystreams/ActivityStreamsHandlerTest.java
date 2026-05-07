package edu.cornell.library.integration.authority.activitystreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.db_test.SqliteBaseTest;

public class ActivityStreamsHandlerTest extends DbBaseTest {
	static Map<String, Path> resources = new LinkedHashMap<>();

	@BeforeAll
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
				SqliteBaseTest.runStmt(Arrays.asList(sql), authority);
			}
		}

		for (String file : Arrays.asList("names_madsrdf_1.json", "names_madsrdf_2.json",
				"subjects_madsrdf_1.json", "names_madsrdf_subdivision_1.json",
				"subjects_madsrdf_subdivision_1.json", "subjects_madsrdf_hai_van_pass.json",
				"names_undifferentiated_1.json", "names_deprecated_1.json",
				"names_name_title_1.json", "subjects_name_title_1.json",
				"subjects_complex_subject_1.json")) {
			Path path = Path.of(base, file);
			resources.put(file, path);
		}
	}

	@Test
	public void parseActivityStreamsTest() throws IOException, InterruptedException, JsonLdError {
		Path path = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority", "names_activitystreams_feed_1.json");
		ActivityStreamsHandler handler = new ActivityStreamsHandler();
		try (InputStream is = Files.newInputStream(path)) {
			ActivityStreams activityStreams = handler.parseActivityStreams(is);
			assertEquals("http://id.loc.gov/authorities/names/activitystreams/feed/1", activityStreams.id);
			assertEquals("http://id.loc.gov/authorities/names/activitystreams/feed/2", activityStreams.next);
			assertEquals(100, activityStreams.orderedItems.size());
			assertEquals("http://id.loc.gov/authorities/names/no2026038416", activityStreams.orderedItems.get(0).id);
			assertEquals("http://id.loc.gov/authorities/names/no2026038416.json", activityStreams.orderedItems.get(0).link);
		}
	}

	@Test
	public void processDataTest() throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		Path localDir = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			ActivityStreamsHandler handler = new ActivityStreamsHandler();
			IFetcher fetcher = new FileFetcher(localDir);
			String addedDate = Utils.getToday();
			ActivityStreamsDatasetEntry entry = ActivityStreamsDataset.getParam("subjects");
			entry.url = "subjects_activity_streams_feed_1.json";
			handler.processData(addedDate, authority, fetcher, entry);
		}
	}
}
