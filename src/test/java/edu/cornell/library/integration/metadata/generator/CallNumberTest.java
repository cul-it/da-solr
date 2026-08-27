package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;

public class CallNumberTest extends DbBaseTest {

//	static Config config = null;
	SolrFieldGenerator gen = new CallNumber();
	static ObjectMapper mapper = new ObjectMapper();

	@BeforeClass
	public static void setup() throws IOException, SQLException {
//		config = Config.loadConfig(new ArrayList<>());
		setup("Headings");
		SupportReferenceData.initializeCallNumberTypes("example_reference_data/call-number-types.json");

	}

	@Test
	public void testCarriageReturnCallNumber() throws IOException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		String holdingJson =
		"{\"id\":\"6f77216e-7ef3-4d61-8de2-6b0cad6b11ff\","
		+ "\"_version\":2,"
		+ "\"hrid\":\"15003285\","
		+ "\"callNumberTypeId\":\"95467209-6d7b-468b-94df-0f5d7ad2747d\","
		+ "\"callNumber\":\"\\nPJ7916.I55999 D43 2019\"}";
		rec.folioHoldings = new ArrayList<>();
		rec.folioHoldings.add((Map<String, Object>) mapper.readValue(holdingJson, Map.class));
		String expected =
		"lc_callnum_full: PJ7916.I55999 D43 2019\n" + 
		"callnum_sort: PJ7916.I55999 D43 2019\n" + 
		"lc_callnum_facet: P - Language & Literature\n" +
		"lc_callnum_facet: P - Language & Literature > PJ - Oriental Philology and Literature\n" + 
		"lc_callnum_facet: P - Language & Literature > PJ - Oriental Philology and Literature > "
		+ "PJ6001-8517 - Arabic\n" + 
		"lc_callnum_facet: P - Language & Literature > PJ - Oriental Philology and Literature > "
		+ "PJ6001-8517 - Arabic > PJ7501-8517 - Arabic literature\n" + 
		"lc_callnum_facet: P - Language & Literature > PJ - Oriental Philology and Literature > "
		+ "PJ6001-8517 - Arabic > PJ7501-8517 - Arabic literature > "
		+ "PJ7695.8-7976 - Individual authors or works\n";
		assertEquals(expected, gen.generateSolrFields(rec, config).toString());
	}
}
