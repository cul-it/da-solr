package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class CitationReferenceNoteTest {

	SolrFieldGenerator gen = new CitationReferenceNote();

	@Test
	public void testReferences() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(3,"510",'3',' ',"‡a Described in DOCUMENTATION NEWSLETTER, Fall 1988."));
		String expected =
		"references_display: Described in DOCUMENTATION NEWSLETTER, Fall 1988.\n"+
		"notes_t: Described in DOCUMENTATION NEWSLETTER, Fall 1988.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testIndexedBy() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(3,"510",'0',' ',"‡a Indexed by note."));
		String expected =
		"indexed_by_display: Indexed by note.\n"+
		"notes_t: Indexed by note.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testIndexedSelectively880() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(3, 1,"510",'2',' ',"‡6 880-01 ‡a Indexed Selectively by XXXXX",false));
		rec.dataFields.add(new DataField(17,1,"510",'2',' ',"‡6 510-01 ‡a Non-Roman Indexed Selectively by XXXXX",true));
		String expected =
		"indexed_selectively_by_display: Non-Roman Indexed Selectively by XXXXX\n"+
		"notes_t: Non-Roman Indexed Selectively by XXXXX\n"+
		"indexed_selectively_by_display: Indexed Selectively by XXXXX\n"+
		"notes_t: Indexed Selectively by XXXXX\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
//		System.out.println(gen.generateSolrFields(rec,null).toString().replaceAll("\"","\\\\\""));
	}

}
