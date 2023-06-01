package edu.cornell.library.integration.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.Generator;

public class GenerateSolrFieldsTest extends DbBaseTest {
	static GenerateSolrFields gen;

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		List<String> additionalRequiredArgs = Config.getRequiredArgsForDB("Headings");
		additionalRequiredArgs.add("catalogClass");
		setup("Headings", additionalRequiredArgs);
		gen = new GenerateSolrFields(
				EnumSet.allOf(Generator.class),
				EnumSet.of(Generator.AUTHORTITLE,Generator.RECORDTYPE,Generator.CALLNUMBER,
						Generator.LANGUAGE, Generator.MARC, Generator.URL), "solrGenTest" );
		gen.setUpDatabase(config);
	}

	@AfterClass
	public static void cleanup() throws SQLException {
		if (useTestContainers == null && useSqlite == null) {
			try (	Connection inventory = config.getDatabaseConnection("Current");
					Statement stmt = inventory.createStatement()) {
				stmt.executeUpdate("DROP TABLE solrGenTestData");
				stmt.executeUpdate("DROP TABLE solrGenTestGenerators");
			}
		}
	}

	@Test
	public void test4087458() throws IOException, XMLStreamException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "4087458.xml" ), true);
		rec.marcHoldings.add(new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h4650028.xml" ), true));
		gen.generateSolr(rec, config, null);
	}

	@Test
	public void testRecordModification() throws IOException, XMLStreamException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "8226661.xml" ), true);
		MarcRecord holdingRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h8616583.xml" ), true);
		rec.marcHoldings.add(holdingRec);
		gen.generateSolr(rec, config, "");
		MarcRecord rec2 = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "8226661-mod.xml" ), true);
		rec2.marcHoldings.add(holdingRec);
		gen.generateSolr(rec2, config, null);
	}

	@Test
	public void identifyForcedGenerators() {
		String reindexCause = "Because of HATHILINKS";
		EnumSet<Generator> forcedGenerators = EnumSet.noneOf(Generator.class);
		forcedGenerators.addAll(
				Arrays.stream(Generator.values())
				.filter(e -> reindexCause.contains(e.name()))
				.collect(Collectors.toSet()));
		assertEquals( EnumSet.of(Generator.HATHILINKS), forcedGenerators );
	}

	@Test
	public void sanitizeInstance() {
		Map<String,Object> instance = new HashMap<>();
		instance.put("We hope to see consecutive spaces cleaned up", "a     a");
		instance.put("As well as literal carriage returns", "a \na");
		instance.put("And symbolic ones", "a \\na");
		Map<String,Object> testContributor = new LinkedHashMap<>();
		testContributor.put("name","Sprague, Ted\n");
		testContributor.put("contributorTypeId","6e09d47d-95e2-4d8a-831b-f777b8ef6d81");
		testContributor.put("contributorNameTypeId","2b94c631-fca9-4892-a730-03ee529ffe2a");
		testContributor.put("primary",true);
		
		testContributor.put("name-xtra1","Sprague, Ted\\n");
		testContributor.put("name-xtra2","Sprague, Ted\\\n");
		testContributor.put("name-xtra3","Sprague, Ted\\\\n");
		testContributor.put("name-xtra4","Sprague, Ted\\\\\n");
		testContributor.put("name-xtra5","Sprague, Ted\\\\\\n");
		testContributor.put("name-xtra6","Sprague, Ted\\\\\\\n");
		instance.put("testContributor", testContributor);

		List<Map<String,Object>> contributorList = new ArrayList<>();
		contributorList.add(testContributor);
		instance.put("contributors", contributorList);

		GenerateSolrFields.sanitizeCarriageReturnsInInstance(instance);

		assertEquals("a a",instance.get("We hope to see consecutive spaces cleaned up"));
		assertEquals("a a",instance.get("As well as literal carriage returns"));
		assertEquals("a a",instance.get("And symbolic ones"));

		Object testContributorOut = instance.get("testContributor");
		assertTrue(Map.class.isInstance(testContributorOut));
		Map<String,Object> contributorOut = Map.class.cast(testContributorOut);
		assertEquals(10,contributorOut.size());
		for (Object key :contributorOut.keySet())
			if (String.class.isInstance(contributorOut.get(key)))
				assertFalse(String.class.cast(contributorOut.get(key)).contains("\n"));

		Object testContributorListOut = instance.get("contributors");
		assertTrue(ArrayList.class.isInstance(testContributorListOut));
		List<Map<String,Object>> contributorsOut = ArrayList.class.cast(testContributorListOut);
		assertEquals(1,contributorsOut.size());
		Map<String,Object> contributorOut2 = contributorsOut.get(0);
		assertEquals(10,contributorOut2.size());
		for (Object key : contributorOut2.keySet())
			if (String.class.isInstance(contributorOut2.get(key)))
				assertFalse(String.class.cast(contributorOut2.get(key)).contains("\n"));
		
	}


	private static String resourceAsString( String filename ) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}

}
