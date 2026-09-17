package edu.cornell.library.integration.authority.mads;

import java.io.FileNotFoundException;
import java.io.IOException;
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
import edu.cornell.library.integration.authority.activitystreams.Utils;
import edu.cornell.library.integration.authority.mads.References.Relationship;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;

public class ReferencesTest extends CustomDbSetupTest {
	static final Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");

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
		testContainersSetupFunctions.add(conn -> {
			try {
				Utils.setUpDatabase(conn);
				DbUtils.setUpHeadingsDatabase(conn);
				DbUtils.setupAuthorityDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		Path extraInit = Path.of(base, "authority_extra_insert.sql");
		extraInsertSqls = Files.readAllLines(extraInit);
		custom_setup();

		DbUtils.fetcher = new IFetcher.FileFetcher(rootPath);
	}

	@Test
	public void testReferences() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
		try ( Connection authority = config.getDatabaseConnection("Headings")) {
			String id = "gf2010025067";
			String uri = JsonldUtils.uri(id);
			AuthorityData latest = DbUtils.authorityRecord(authority, uri);
			String authLabel = JsonldUtils.getAuthorativeLabel(latest.mainEntry);
			HeadingType t = JsonldUtils.headingType(latest.mainEntry, latest.id);
			Heading mainHead = Heading.processHeading(authority, latest.mainEntry, latest.graph, authLabel, null, t);

			// sees
			List<Relationship> sees = References.sees(authority, latest.mainEntry, latest.graph, uri, mainHead);
			assert sees.size() == 1;
			var rel = sees.get(0);
			assert rel.relationship() == null;
			assert rel.reciprocalRelationship() == null;
			assert rel.heading() != null;
			assert rel.heading().heading().compareTo("Gravity maps") == 0;
			assert rel.heading().headingType() == HeadingType.GENRE_FORM;

			// seeAlsos
			List<Relationship> seeAlsos = References.seeAlsos(authority, latest.mainEntry, latest.graph, uri, mainHead);
			assert seeAlsos.size() == 1;
			rel = seeAlsos.get(0);
			assert rel.relationship().compareTo("Narrower Term") == 0;
			assert rel.reciprocalRelationship().compareTo("Broader Term") == 0;
			assert rel.heading() != null;
			assert rel.heading().heading().compareTo("Maps") == 0;
			assert rel.heading().headingType() == HeadingType.GENRE_FORM;
		}
	}
}
