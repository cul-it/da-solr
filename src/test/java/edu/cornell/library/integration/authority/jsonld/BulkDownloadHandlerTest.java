package edu.cornell.library.integration.authority.jsonld;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
				DBHelper.setUpDatabase(authority);
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
	public void loadBulkJsonLDTest() throws IOException, SQLException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		for (Path path : resources.values()) {
			String raw = Files.readString(path).replaceAll("\\R", "");
			builder.append(raw).append(System.lineSeparator());
		}
		InputStream inputStream = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
		BulkDownloadHandler lbd = new BulkDownloadHandler();
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  PreparedStatement stmt = authority.prepareStatement("SELECT * FROM %s WHERE id = ? AND moddate = ?".formatted(DBHelper.UPDATE_TABLE))) {
			String newCursor = lbd.loadBulkJsonLD(inputStream, authority);
			assertEquals("2025-08-08T16:31:10", newCursor);
			DBHelper.updateCursor(authority, newCursor);
			String cursor = DBHelper.getCursor(authority);
			assertEquals(newCursor, cursor);

			stmt.setString(1, "http://id.loc.gov/authorities/names/n00000001");
			stmt.setString(2, "2025-08-05T02:34:09");
			ResultSet rs = stmt.executeQuery();
			assertEquals(true, rs.next());
			assertEquals("n 00000001", rs.getString("lccn"));
			assertEquals(AuthoritySource.valueOf("NAF").ordinal(), rs.getInt("vocabulary"));
			assertEquals(MadsRecordStatus.valueOf("REVISED").ordinal(), rs.getInt("recordStatus"));
			assertEquals("McQuerry, Maureen, 1955-", rs.getString("heading"));
			assertEquals(MadsHeadingType.valueOf("PERSONAL_NAME").ordinal(), rs.getInt("headingType"));
			assertEquals(false, rs.getBoolean("isSubdivision"));
			assertEquals(false, rs.getBoolean("undifferentiated"));
		}
	}

	@Test
	public void parseBulkEntryTest() throws IOException, JsonLdError, URISyntaxException {
		BulkDownloadHandler lbd = new BulkDownloadHandler();
		String content = Files.readString(resources.get("names_madsrdf_1.json"));
		AuthorityParsedData data = lbd.parseBulkEntry(content);
		assertEquals("http://id.loc.gov/authorities/names/n00000001", data.id);
		assertEquals("n 00000001", data.lccn);
		assertEquals(AuthoritySource.valueOf("NAF"), data.vocab);
		assertEquals(MadsRecordStatus.valueOf("REVISED"), data.recordStatus);
		assertEquals("McQuerry, Maureen, 1955-", data.authorativeLabel);
		assertEquals(MadsHeadingType.valueOf("PERSONAL_NAME"), data.headingType);
		assertEquals(false, data.isSubdivision);
		assertEquals(false, data.undifferentiated);
		assertEquals("2025-08-05T02:34:09", data.moddate);

		content = Files.readString(resources.get("names_madsrdf_2.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals("Belʹdiĭ, A. I︠A︡.", data.authorativeLabel);

		content = Files.readString(resources.get("subjects_madsrdf_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(AuthoritySource.valueOf("LCSH"), data.vocab);
		assertEquals("ActionScript (Computer program language)", data.authorativeLabel);
		assertEquals(MadsHeadingType.valueOf("TOPIC"), data.headingType);

		content = Files.readString(resources.get("names_madsrdf_subdivision_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(null, data.lccn);
		assertEquals("Germany > Marienstatt", data.authorativeLabel);
		assertEquals(MadsHeadingType.valueOf("HIERARCHICAL_GEOGRAPHIC"), data.headingType);

		content = Files.readString(resources.get("subjects_madsrdf_subdivision_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals("sh 00008433", data.lccn);
		assertEquals("Reinstatement", data.authorativeLabel);
		assertEquals(MadsHeadingType.valueOf("TOPIC"), data.headingType);
		assertEquals(true, data.isSubdivision);

		content = Files.readString(resources.get("subjects_madsrdf_hai_van_pass.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals("Hải Vân Pass (Vietnam)", data.authorativeLabel);
		assertEquals(MadsHeadingType.valueOf("GEOGRAPHIC"), data.headingType);
		assertEquals(false, data.isSubdivision);

		content = Files.readString(resources.get("names_undifferentiated_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals("Mason, Jack", data.authorativeLabel);
		assertEquals(true, data.undifferentiated);

		content = Files.readString(resources.get("names_deprecated_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(null, data.lccn);
		assertEquals(MadsRecordStatus.valueOf("DEPRECATED"), data.recordStatus);
		assertEquals(null, data.authorativeLabel);

		content = Files.readString(resources.get("names_name_title_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(MadsHeadingType.valueOf("NAME_TITLE"), data.headingType);

		// Following two have -- in authoratativeLabel but is not subdivision
		content = Files.readString(resources.get("subjects_name_title_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(MadsHeadingType.valueOf("NAME_TITLE"), data.headingType);
		assertEquals("Texas. Declaration of Independence--Signers", data.authorativeLabel);

		content = Files.readString(resources.get("subjects_complex_subject_1.json"));
		data = lbd.parseBulkEntry(content);
		assertEquals(MadsHeadingType.valueOf("COMPLEX_SUBJECT"), data.headingType);
		assertEquals("Space vehicles--Doppler tracking", data.authorativeLabel);
	}
}
