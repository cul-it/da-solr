package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumSet;
import java.util.List;
import java.util.Scanner;

import javax.xml.stream.XMLStreamException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.indexer.utilities.Generator;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class GenerateSolrFieldsTest {

	static Config config;
	static GenerateSolrFields gen;

	@BeforeClass
	public static void setup() throws ClassNotFoundException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		config = Config.loadConfig(null,requiredArgs);
		gen = new GenerateSolrFields(	EnumSet.allOf(Generator.class), "solrGenTest" );
		gen.setUpDatabase(config);
	}

	@AfterClass
	public static void cleanup() throws ClassNotFoundException, SQLException {
		try (   Connection inventory = config.getDatabaseConnection("Current");
				Statement stmt = inventory.createStatement()) {
			stmt.executeUpdate("DROP TABLE solrGenTestData");
			stmt.executeUpdate("DROP TABLE solrGenTestGenerators");
		}
	}

	@Test
	public void test4087458() throws IOException, XMLStreamException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "4087458.xml" ));
		rec.holdings.add(new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h4650028.xml" )));
		gen.generateSolr(rec, config, null);
	}

	@Test
	public void testRecordModification() throws IOException, XMLStreamException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "8226661.xml" ));
		MarcRecord holdingRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h8616583.xml" ));
		rec.holdings.add(holdingRec);
		gen.generateSolr(rec, config, "");
		MarcRecord rec2 = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "8226661-mod.xml" ));
		rec2.holdings.add(holdingRec);
		gen.generateSolr(rec2, config, null);
	}

	private static String resourceAsString( String filename ) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}

}
