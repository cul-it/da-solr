package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;

public class OtherIDsTest {

	SolrFieldGenerator gen = new OtherIDs();

	@Before
	public void before() throws IOException {
		SupportReferenceData.initializeIdentifierTypes("example_reference_data/identifier-types.json");
	}

	@Test
	public void test035() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)924835975"));
		String expected =
		"id_t: (OCoLC)924835975\n"+
		"oclc_id_display: 924835975\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}


	@Test
	public void testDoi() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"024",'7',' ',"‡a 10.1007/978-3-662-46444-1 ‡2 doi"));
		rec.dataFields.add(new DataField(2,"035",' ',' ',"‡a (WaSeSS)OCM1ssj0001465585"));
		rec.dataFields.add(new DataField(3,"035",' ',' ',"‡a (OCoLC)904397987"));
		rec.dataFields.add(new DataField(4,"035",' ',' ',"‡a 8903327"));
		String expected =
		"id_t: 10.1007/978-3-662-46444-1\n" + 
		"doi_display: 10.1007/978-3-662-46444-1\n" + 
		"id_t: (WaSeSS)OCM1ssj0001465585\n" + 
		"other_id_display: (WaSeSS)OCM1ssj0001465585\n" + 
		"id_t: (OCoLC)904397987\n" + 
		"oclc_id_display: 904397987\n" + 
		"id_t: 8903327\n" + 
		"other_id_display: 8903327\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testDiscogs() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"024",'7',' ',"‡a test id value ‡2 discogs"));
		String expected =
		"id_t: test id value\n" + 
		"discogs_display: test id value\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void instanceOCLCNumber() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("identifiers", Arrays.asList(
				new HashMap<String,Object>() {{
					put("value","(OCoLC)156993816");
					put("identifierTypeId",SupportReferenceData.identifierTypes.getUuid("OCLC"));  }},
				new HashMap<String,Object>() {{
					put("value","231-B");
					put("identifierTypeId",SupportReferenceData.identifierTypes.getUuid("GPO item number"));  }},
				new HashMap<String,Object>() {{
					put("value","ATHM_Post1930_noCULholdings");
					put("identifierTypeId",SupportReferenceData.identifierTypes.getUuid("Collection ID"));  }}
				));
		String expected =
		"id_t: (OCoLC)156993816\n"+
		"oclc_id_display: 156993816\n";
		assertEquals(expected,this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void instancePublisherNumber() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("identifiers", Arrays.asList(
				new HashMap<String,Object>() {{
					put("value","M.CD-422");
					put("identifierTypeId",
							SupportReferenceData.identifierTypes.getUuid("Publisher or distributor number"));  }}
				));
		String expected =
		"id_t: M.CD-422\n"+
		"publisher_number_display: M.CD-422\n";
		assertEquals(expected,this.gen.generateNonMarcSolrFields(instance, null).toString());
	}
}
