package edu.cornell.library.integration.authority.mads;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.activitystreams.IFetcher;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;
import edu.cornell.library.integration.db_test.DbBaseTest;

public class NotesTest extends CustomDbSetupTest {
	static final Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");
	private final static ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();
    private final static PrintStream originalOut = System.out;

	/*
		Test data
		gf2010025067 - sees: , seeAlsos: Narrower, Broader
		n00000001 - rda occupation
		n00000203 - rda birthPlace, occupation with rdf:label Author, affiliation, field
		n00000264 - rda in rwo
		n00002040 - notes: 663 (most common)
		n00014709 - has a variant that is also an earlier form that we don't display
		n50071532 - notes: 665
		n78015928 - 500 $w f - Based On
		sh2001001501 - parent heading
		sh2010004770 - 
	 */

	@BeforeAll
	public static void setup_test() throws Exception {
		String base = rootPath.toString();
		sqliteExtraSql = Path.of(base, "authority_extra_sqlite_create.sql").toString();
		custom_setup();

		Path extraInit = Path.of(base, "authority_extra_insert.sql");
		List<String> extraSqlLines = Files.readAllLines(extraInit);
		try (Connection conn = config.getDatabaseConnection("Authority")) {
			DbBaseTest.executeStmt(extraSqlLines, conn);
		}

		DbUtils.fetcher = new IFetcher.FileFetcher(rootPath, true);
	}

	@Test
	public void testNotes() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
		try ( Connection authority = config.getDatabaseConnection("Headings")) {
			String id = "n00002040";
			String uri = JsonldUtils.uri(id);
			System.setOut(new PrintStream(outputStreamCaptor));
			AuthorityData latest = DbUtils.authorityRecord(authority, uri);
			var notes = Notes.notes(authority, latest.mainEntry, latest.graph);
			System.setOut(originalOut);
			assert notes.get(0).compareTo("Search also under: [{\"header\":\"Hart, A. W.\"},{\"header\":\"Pendleton, Don, 1927-1995\"}]") == 0;
			assert outputStreamCaptor.toString().contains("Authority data id http://id.loc.gov/authorities/names/n2020004346 not found in DB, fetched from remote.");
			assert outputStreamCaptor.toString().contains("Authority data id http://id.loc.gov/authorities/names/n87930300 not found in DB, fetched from remote.");
		}
	}
}
