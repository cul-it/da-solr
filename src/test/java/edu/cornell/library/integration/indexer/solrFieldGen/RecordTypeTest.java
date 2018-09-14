package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class RecordTypeTest {

	SolrFieldGenerator gen = new RecordType();

	@Test
	public void testStandardCatalogRecord() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'2',' ',
				"‡a 20141130 ‡b m ‡d batch ‡e lts ‡x addfast"));
		String expected =
		"type: Catalog\n"+
		"source: Voyager\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testShadowRecordBibIndicator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20050809 ‡b o ‡d str1 ‡e lts ‡h PUBLIC SERVICES SHADOW RECORD"));
		String expected =
		"type: Shadow\n"+
		"source: Voyager\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testShadowRecordHoldingsIndicator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "6161049";
		hRec.dataFields.add(new DataField(1,"852",'8','1',
				"‡b olin ‡h See link below ‡x PUBLIC SERVICES SHADOW RECORD"));
		rec.holdings.add(hRec);
		String expected =
		"type: Shadow\n"+
		"source: Voyager\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}
}
