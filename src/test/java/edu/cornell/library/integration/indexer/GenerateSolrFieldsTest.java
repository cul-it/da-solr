package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Scanner;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.utilities.Generator;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class GenerateSolrFieldsTest {

	static SolrBuildConfig config = null;
	static GenerateSolrFields gen = new GenerateSolrFields(	EnumSet.allOf(Generator.class) );

	@BeforeClass
	public static void setup() throws ClassNotFoundException, SQLException {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
		gen.setUpDatabase(config);
	}

	@Test
	public void test4087458() throws IOException, XMLStreamException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "4087458.xml" ));
		rec.holdings.add(new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h4650028.xml" )));
		gen.generateSolr(rec, config);
	}

	@Test
	public void test8226661() throws IOException, XMLStreamException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "8226661.xml" ));
		rec.holdings.add(new MarcRecord( MarcRecord.RecordType.HOLDINGS, resourceAsString( "h8616583.xml" )));
		gen.generateSolr(rec, config);
	}

	private static String resourceAsString( String filename ) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}

}
