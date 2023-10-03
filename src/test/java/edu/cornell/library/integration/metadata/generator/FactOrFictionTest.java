package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.MarcRecord;

public class FactOrFictionTest {

	SolrFieldGenerator gen = new FactOrFiction();

	@Test
	public void testFictionBook() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = "00747cam a2200229Ii 4500";
		rec.controlFields.add(new ControlField(1,"008","170404s2017    vm            000 f vie  "));
		assertEquals("subject_content_facet: Fiction (books)\n",
				this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNonFictionBook() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = "00643cam a22001935i 4500";
		rec.controlFields.add(new ControlField(1,"008","170113s2017    ic     e          0 ice c"));
		assertEquals("subject_content_facet: Non-Fiction (books)\n",
				this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	// The 008/33 flag would mean fiction if a book, but this is a journal
	public void testJapaneseJournal() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = "03200cas a2200649 i 4500";
		rec.controlFields.add(new ControlField(1,"008","120817d19361944ja uu   r     0   d0jpn d"));
		assertEquals("",this.gen.generateSolrFields ( rec, null ).toString());
	}

}
