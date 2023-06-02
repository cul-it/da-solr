package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;

public class RecordTypeTest {

	SolrFieldGenerator gen = new RecordType();

	@Test
	public void testStandardCatalogRecord() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'2',' ',
				"‡a 20141130 ‡b m ‡d batch ‡e lts ‡x addfast"));
		String expected =
		"type: Catalog\n"+
		"source: Folio\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testShadowRecordBibIndicator() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20050809 ‡b o ‡d str1 ‡e lts ‡h PUBLIC SERVICES SHADOW RECORD"));
		String expected =
		"type: Shadow\n"+
		"source: Folio\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testShadowRecordHoldingsIndicator() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "6161049";
		hRec.dataFields.add(new DataField(1,"852",'8','1',
				"‡b olin ‡h See link below ‡x PUBLIC SERVICES SHADOW RECORD"));
		rec.marcHoldings.add(hRec);
		String expected =
		"type: Shadow\n"+
		"source: Folio\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void statisticalCodes() throws SQLException, IOException {
		StatisticalCodes.codes = new ReferenceData("code");
		StatisticalCodes.codes.addTestValue("7509bbd4-9fb7-4fb7-ab65-cc4017709e2d", "test-code");
		Map<String,Object> instance = new HashMap<>();
		instance.put("statisticalCodeIds", Arrays.asList("7509bbd4-9fb7-4fb7-ab65-cc4017709e2d"));
		String expected =
		"type: Non-MARC Instance\n" + 
		"source: Folio\n" + 
		"statcode_facet: instance_test-code\n";
		assertEquals(expected,this.gen.generateNonMarcSolrFields(instance, null).toString());
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.instance = instance;
		expected =
		"type: Catalog\n" + 
		"source: Folio\n" + 
		"statcode_facet: instance_test-code\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}
}
