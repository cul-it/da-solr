package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class DBCodeTest {

	SolrFieldGenerator gen = new DBCode();

	@Test
	public void testReferences() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"899",'2',' ',"‡a PRVLSH_ACAJP"));
		rec.dataFields.add(new DataField(2,"899",' ',' ',"‡a Marcive"));
		String expected =
		"providercode: PRVLSH\n"+
		"dbcode: ACAJP\n";
		assertEquals( expected, gen.generateSolrFields(rec,null).toString());
	}
}
