package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import javax.xml.stream.XMLStreamException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class GenerateSolrFieldsTest {

	static SolrBuildConfig config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
	}

	@Test
	public void test4087458() throws IOException, XMLStreamException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC, resourceAsString( "4087458.xml" ));
		GenerateSolrFields.generateSolr(rec, config);
	}

	private static String resourceAsString( String filename ) {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		} catch (@SuppressWarnings("unused") IOException e) { return null; }
		
	}

}
