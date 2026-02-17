package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.StatisticalCodes;

public class RecordTypeTest {

	SolrFieldGenerator gen = new RecordType();

	@Before
	public void populateHardcodedStatCodes() {
		StatisticalCodes.codes = new ReferenceData("code");
		StatisticalCodes.codes.addTestValue("7509bbd4-9fb7-4fb7-ab65-cc4017709e2d", "test-code");
		StatisticalCodes.codes.addTestValue("no-google-img-code-uuid", "no-google-img");
		StatisticalCodes.codes.addTestValue("no-syndetics-code-uuid", "no-syndetics");

	}

	@Test
	public void testStandardCatalogRecord() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'2',' ',
				"‡a 20141130 ‡b m ‡d batch ‡e lts ‡x addfast"));
		rec.instance = new HashMap<String,Object>();
		String expected =
		"type: Catalog\n"+
		"source: MARC\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testShadowRecordBibIndicator() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20050809 ‡b o ‡d str1 ‡e lts ‡h PUBLIC SERVICES SHADOW RECORD"));
		rec.instance = new HashMap<String,Object>();
		String expected =
		"type: Shadow\n"+
		"source: MARC\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}


	@Test
	public void statisticalCodes() throws SQLException, IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("statisticalCodeIds", Arrays.asList("7509bbd4-9fb7-4fb7-ab65-cc4017709e2d"));
		instance.put("source", "FOLIO");
		String expected =
		"type: Hidden not suppressed\n" + 
		"source: FOLIO\n" + 
		"statcode_facet: instance_test-code\n";
		assertEquals(expected,this.gen.generateNonMarcSolrFields(instance, null).toString());
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.instance = instance;
		expected =
		"type: Catalog\n" + 
		"source: MARC\n" + 
		"statcode_facet: instance_test-code\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void integrationSuppressions() throws SQLException, IOException {

		// non-marc instance
		Map<String,Object> instance = new HashMap<>();
		instance.put("statisticalCodeIds", Arrays.asList("no-google-img-code-uuid"));
		instance.put("source", "FOLIO");
		String expected =
		"type: Hidden not suppressed\n"+
		"source: FOLIO\n"+
		"statcode_facet: instance_no-google-img\n"+
		"no_google_img_b: true\n";
		assertEquals(expected, this.gen.generateNonMarcSolrFields(instance, null).toString());

		// marc instance
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.instance = new HashMap<>();
		rec.instance.put("statisticalCodeIds", Arrays.asList("no-syndetics-code-uuid"));
		rec.instance.put("source", "MARC");
		expected =
		"type: Catalog\n"+
		"source: MARC\n"+
		"statcode_facet: instance_no-syndetics\n"+
		"no_syndetics_b: true\n";
		assertEquals(expected, this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void nonMarcWithoutHolding() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("source", "FOLIO");
		String expected = 
		"type: Hidden not suppressed\n"+
		"source: FOLIO\n";
		assertEquals(expected, this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void nonMarcWithHolding() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("source", "FOLIO");
		Map<String,Object> holding = new HashMap<>();
		List<Map<String,Object>> holdings = new ArrayList<Map<String,Object>>();
		holdings.add(holding);
		instance.put("holdings", holdings);
		String expected = 
		"type: Catalog\n"+
		"source: FOLIO\n";
		assertEquals(expected, this.gen.generateNonMarcSolrFields(instance, null).toString());
	}


	@Test
	public void nonMarcWithHoldingButWrongSource() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("source", "bd");
		Map<String,Object> holding = new HashMap<>();
		List<Map<String,Object>> holdings = new ArrayList<Map<String,Object>>();
		holdings.add(holding);
		instance.put("holdings", holdings);
		String expected = 
		"type: Hidden not suppressed\n"+
		"source: bd\n";
		assertEquals(expected, this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void nonMarcWithSuppressedHolding() throws IOException {
		Map<String,Object> instance = new HashMap<>();
		instance.put("source", "FOLIO");
		Map<String,Object> holding = new HashMap<>();
		holding.put("discoverySuppress", true);
		List<Map<String,Object>> holdings = new ArrayList<Map<String,Object>>();
		holdings.add(holding);
		instance.put("holdings", holdings);
		String expected = 
		"type: Hidden not suppressed\n"+
		"source: FOLIO\n";
		assertEquals(expected, this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

}
