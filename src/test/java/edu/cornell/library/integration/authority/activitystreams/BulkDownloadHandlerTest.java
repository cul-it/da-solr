package edu.cornell.library.integration.authority.activitystreams;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.AuthoritySource;
import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.db_test.SqliteBaseTest;

public class BulkDownloadHandlerTest extends DbBaseTest {
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
	public void loadBulkJsonLDTest() throws IOException, InterruptedException, JsonLdError, SQLException, URISyntaxException {
		StringBuilder builder = new StringBuilder();
		/*
		 * Combine the individual test data to simulate the bulk download.
		 * Each line should contain complete data for a single record.
		 */
		for (Path path : resources.values()) {
			String raw = Files.readString(path).replaceAll("\\R", "");
			builder.append(raw).append(System.lineSeparator());
		}
		Path tempFile = Files.createTempFile("myPrefix", ".tmp");
		try (OutputStream out = Files.newOutputStream(tempFile);
			 Connection authority = config.getDatabaseConnection("Authority");
			 PreparedStatement stmt = authority.prepareStatement("SELECT * FROM %s WHERE id = ? AND numUpdates = ? AND moddate = ?".formatted(Utils.UPDATE_TABLE))) {
			out.write(builder.toString().getBytes(StandardCharsets.UTF_8));

			String today = Utils.getToday();
			BulkDownloadHandler bdh = new BulkDownloadHandler();
			bdh.processData(today, tempFile, authority, 2);

			stmt.setString(1, "http://id.loc.gov/authorities/names/n00000001");
			stmt.setInt(2, 2);
			stmt.setString(3, "2025-08-05T02:34:09");
			ResultSet rs = stmt.executeQuery();
			assertEquals(true, rs.next());
			assertEquals("n 00000001", rs.getString("lccn"));
			assertEquals(AuthoritySource.valueOf("NAF").ordinal(), rs.getInt("vocabulary"));
			assertEquals(MadsRecordStatus.valueOf("REVISED").ordinal(), rs.getInt("recordStatus"));
			assertEquals("McQuerry, Maureen, 1955-", rs.getString("heading"));
			assertEquals(MadsHeadingType.valueOf("PERSONAL_NAME").ordinal(), rs.getInt("headingType"));
			assertEquals(false, rs.getBoolean("isSubdivision"));
			assertEquals(false, rs.getBoolean("undifferentiated"));
			assertEquals(today, rs.getString("addedDate"));
			assertEquals(2, rs.getInt("numUpdates"));
		} finally {
			Files.delete(tempFile);
		}
	}
}
