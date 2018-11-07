package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.MarcRecord;

public class RecordBoostTest {

	SolrFieldGenerator gen = new RecordBoost();

	@Test
	public void testShadowRecordLinked() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "5384546";
		assertEquals( "^100\n", gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testUnboosted() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "5";
		assertEquals( "", gen.generateSolrFields ( rec, null ).toString());
	}

}
