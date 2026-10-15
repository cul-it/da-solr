package edu.cornell.library.integration.authority.activitystreams;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.mads.DbUtils;
import edu.cornell.library.integration.authority.mads.HeadingType;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;

public class HandlerTest extends CustomDbSetupTest {
	static Map<String, Path> resources = new LinkedHashMap<>();
	static String addedDate = Utils.getToday();
	static Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");

	@BeforeAll
	public static void setup() throws IOException, SQLException, InterruptedException, JsonLdError, URISyntaxException {
		String base = rootPath.toString();
		sqliteExtraSql = Path.of(base, "authority_extra_sqlite_create.sql").toString();
		testContainersSetupFunctions.add(conn -> {
			try {
				Utils.setUpDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		clearFunctions.add(conn -> {
			try {
				Utils.clearDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		custom_setup();

		for (String file : Arrays.asList("names_madsrdf_1.json", "names_madsrdf_2.json",
				"subjects_madsrdf_1.json", "names_madsrdf_subdivision_1.json",
				"subjects_madsrdf_subdivision_1.json", "subjects_madsrdf_hai_van_pass.json",
				"names_undifferentiated_1.json", "names_deprecated_1.json",
				"names_name_title_1.json", "subjects_name_title_1.json",
				"subjects_complex_subject_1.json")) {
			Path path = Path.of(base, file);
			resources.put(file, path);
		}
		
		DbUtils.fetcher = new IFetcher.FileFetcher(rootPath);
	}

	@Test
	public void parseActivityStreamsTest() throws IOException, InterruptedException, JsonLdError {
		Path path = Path.of(rootPath.toString(), "names_activity_streams_feed_1.json");
		var handlerConfig = new IHandlerConfig.HandlerConfigT(addedDate, 100, "LCNAF", rootPath, null);
		Handler handler = new Handler(handlerConfig);
		try (InputStream is = Files.newInputStream(path)) {
			ActivityStreams activityStreams = handler.parseActivityStreams(is);
			assertEquals("http://id.loc.gov/authorities/names/activitystreams/feed/1", activityStreams.id);
			assertEquals("http://id.loc.gov/authorities/names/activitystreams/feed/2", activityStreams.next);
			assertEquals(2, activityStreams.orderedItems.size());
			assertEquals("http://id.loc.gov/authorities/names/no2026038416", activityStreams.orderedItems.get(0).id);
			assertEquals("http://id.loc.gov/authorities/names/no2026038416.json", activityStreams.orderedItems.get(0).link);
		}
	}

	@Test
	public void processDataTest() throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		/*
		 * The real activity streams feed will only contain a single type.
		 * To increase test coverage, we are faking a feed that contains both name and subject.
		 */
		var handlerConfig = new IHandlerConfig.HandlerConfigT(addedDate, 100, "LCNAF", rootPath, "mixed_activity_streams_feed_1.json");
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			Handler handler = new Handler(handlerConfig);
			handler.processData(authority);
			var ids = DbUtils.identifiers(authority).stream().sorted().toList();
			String id = ids.get(0);
			var doc = DbUtils.authorityRecord(authority, id);
			assert doc.id.equalsIgnoreCase("http://id.loc.gov/authorities/names/no2026038416");
			assert doc.authorativeLabel.equalsIgnoreCase("Ferras, Patrick, 1958-");
			assert doc.headingType == HeadingType.PERSONAL_NAME;

			id = ids.get(1);
			doc = DbUtils.authorityRecord(authority, id);
			assert doc.id.equalsIgnoreCase("http://id.loc.gov/authorities/subjects/sh97007140");
			assert doc.authorativeLabel.equalsIgnoreCase("Judeo-Tat language");
			assert doc.headingType == HeadingType.TOPIC;
		}
	}
}
