package edu.cornell.library.integration.authority.mads;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.IndexAuthorityRecords;
import edu.cornell.library.integration.authority.IndexAuthorityRecords.AuthorityData;
import edu.cornell.library.integration.authority.activitystreams.ActivityStreamsUtils;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;
import edu.cornell.library.integration.marc.MarcRecord;

public class AuthorityMapTest extends CustomDbSetupTest {
	static Map<String, Path> resources = new LinkedHashMap<>();
	static final Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");

	/*
		Test data
		gf2010025067 - sees: , seeAlsos: Narrower, Broader
		n00000001 - rda occupation
		n00000203 - rda birthPlace, occupation with rdf:label Author, affiliation, field
		n00000264 - rda in rwo
		n00002040 - notes: 663 (most common)
		n00014709 - 
		n50071532 - notes: 665
		sh2001001501 - 
		sh2010004770 - 
	 */

	static final List<String> extensions = Arrays.asList(".madsrdf.json", ".marcxml.xml");
	static final Map<String, String> ids = Map.of(
		"gf2010025067", "http://id.loc.gov/authorities/genreForms/gf2010025067",
		"n00000001", "http://id.loc.gov/authorities/names/n00000001",
		"n00000203", "http://id.loc.gov/authorities/names/n00000203",
		"n00000264", "http://id.loc.gov/authorities/names/n00000264",
		"n00002040", "http://id.loc.gov/authorities/names/n00002040",
		"n00002659", "http://id.loc.gov/authorities/names/n00002659",
		"n00014709", "http://id.loc.gov/authorities/names/n00014709",
		"n50071532", "http://id.loc.gov/authorities/names/n50071532",
		"sh2001001501", "http://id.loc.gov/authorities/subjects/sh2001001501",
		"sh2010004770", "http://id.loc.gov/authorities/subjects/sh2010004770"
	);
	static final List<String> files = ids.keySet().stream().toList();

	@BeforeAll
	public static void setup_test() throws Exception {
		String base = rootPath.toString();
		sqliteExtraSql = Path.of(base, "authority_extra_sqlite_create.sql").toString();
		custom_setup();

		for (String file : files) {
			for (String ext : extensions) {
				String fileName = file + ext;
				Path path = Path.of(base, fileName);
				resources.put(fileName, path);
			}
		}

		try (Connection conn = config.getDatabaseConnection("Authority");
			PreparedStatement insertStmt = ActivityStreamsUtils.replaceStmt(conn)) {
			String addedDate = ActivityStreamsUtils.getToday();
			for (String file : files)
				add_resource(file, insertStmt, addedDate);
			insertStmt.executeBatch();
		}

		// AuthorityMap.fetcher = new IFetcher.FileFetcher(rootPath);
	}

	protected static void add_resource(String file, PreparedStatement insertStmt, String addedDate) throws IOException, SQLException, JsonLdError, URISyntaxException {
		String content = Files.readString(resources.get(file + ".madsrdf.json"));
		AuthorityDataMadsSimple parsed;
		if (content.startsWith("{")) {
			parsed = AuthorityJsonldUtils.parseAuthorityData(content);
		} else {
			String id = ids.get(file);
			parsed = AuthorityJsonldUtils.parseAuthorityData(content, id);
		}
		ActivityStreamsUtils.addBatch(insertStmt, parsed, addedDate);
	}

	public static void populateStaticData(Connection conn) throws SQLException {
		AuthorityDbUtils.populateStaticData(conn);
	}

	public static AuthorityData getData(Path filePath) throws IOException, XMLStreamException {
		String content = Files.readString(filePath);
		var rec = new MarcRecord(MarcRecord.RecordType.AUTHORITY, content, true);
		return IndexAuthorityRecords.parseMarcRecord(rec);
	}

	// @Test
	// public void testParent() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
	// 	try ( Connection headings = config.getDatabaseConnection("Headings") ) {
	// 		String id = "sh2001001501";
	// 		String uri = ids.get(id);
	// 		AuthorityDataMadsSimple rec = AuthorityDbUtils.authorityRecordMostRecent(headings, uri);
	// 		AuthorityMap map = AuthorityMap.fromMadsJsonld(headings, uri, rec.source, rec.undifferentiated);
	// 		map.print();
	// 		System.out.println("--------------------");
			
	// 		AuthorityData ad = getData(resources.get(id + ".marcxml.xml"));
	// 		ad.print();

	// 		assert map.mainHead().parent().heading().equalsIgnoreCase(ad.mainHead.parent().displayForm());
	// 	}
	// }

	// @Test
	// public void testSubdivisionSkip() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
	// 	try ( Connection headings = config.getDatabaseConnection("Headings") ) {
	// 		String uri = ids.get("sh2010004770");
	// 		System.out.println("URI: " + uri);
	// 		AuthorityDataMadsSimple rec = AuthorityDbUtils.authorityRecordMostRecent(headings, uri);
	// 		AuthorityMap map = AuthorityMap.fromMadsJsonld(headings, uri, rec.source, rec.undifferentiated);
	// 		assert map == null;
			
	// 		AuthorityData ad = getData(resources.get("sh2010004770.marcxml.xml"));
	// 		ad.print();
	// 	}
	// }

	// @Test
	// public void testNotes() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
	// 	try ( Connection headings = config.getDatabaseConnection("Headings") ) {
	// 		String uri = ids.get("n00002040");
	// 		System.out.println("URI: " + uri);
	// 		AuthorityDataMadsSimple rec = AuthorityDbUtils.authorityRecordMostRecent(headings, uri);
	// 		AuthorityMap map = AuthorityMap.fromMadsJsonld(headings, uri, rec.source, rec.undifferentiated);
	// 		map.print();
	// 		System.out.println("--------------------");
			
	// 		AuthorityData ad = getData(resources.get("n00002040.marcxml.xml"));
	// 		ad.print();
	// 		IndexAuthorityRecords.processAuthorityMarc(headings, ad);
	// 	}
	// }

	@Test
	public void testReferences() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
		try ( Connection headings = config.getDatabaseConnection("Headings") ) {
			String id = "n00014709";
			String uri = ids.get(id);
			System.out.println("URI: " + uri);
			AuthorityDataMadsSimple rec = AuthorityDbUtils.authorityRecordMostRecent(headings, uri);
			MadsAuthority map = MadsAuthority.fromMadsJsonld(headings, uri, rec.source, rec.undifferentiated);
			map.print();
			System.out.println("--------------------");
			
			AuthorityData ad = getData(resources.get(id + ".marcxml.xml"));
			ad.print();
			IndexAuthorityRecords.processAuthorityMarc(headings, ad);
		}
	}

	// @Test
	// public void testRda() throws SQLException, FileNotFoundException, IOException, JsonLdError, URISyntaxException, XMLStreamException, InterruptedException {
	// 	try ( Connection headings = config.getDatabaseConnection("Headings") ) {
	// 		String id = "n00000264";
	// 		String uri = ids.get(id);
	// 		System.out.println("URI: " + uri);
	// 		AuthorityDataMadsSimple rec = AuthorityDbUtils.authorityRecordMostRecent(headings, uri);
	// 		AuthorityMap map = AuthorityMap.fromMadsJsonld(headings, uri, rec.source, rec.undifferentiated);
	// 		map.print();
	// 		System.out.println("--------------------");
			
	// 		id = "n00000264";
	// 		AuthorityData ad = getData(resources.get(id + ".marcxml.xml"));
	// 		ad.print();
	// 		IndexAuthorityRecords.processAuthorityMarc(headings, ad);
	// 	}
	// }
}
