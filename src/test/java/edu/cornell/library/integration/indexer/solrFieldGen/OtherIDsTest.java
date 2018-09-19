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

}
