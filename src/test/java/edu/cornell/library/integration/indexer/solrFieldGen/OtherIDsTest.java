package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class OtherIDsTest {

	SolrFieldGenerator gen = new OtherIDs();


	@Test
	public void test035() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)924835975"));
		String expected =
		"id_t: (OCoLC)924835975\n"+
		"oclc_id_display: 924835975\n";
		assertEquals(expected,gen.generateSolrFields(rec,null).toString());
	}


	@Test
	public void testDoi() throws ClassNotFoundException, SQLException, IOException {
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
		assertEquals(expected,gen.generateSolrFields(rec,null).toString());
	}

}
