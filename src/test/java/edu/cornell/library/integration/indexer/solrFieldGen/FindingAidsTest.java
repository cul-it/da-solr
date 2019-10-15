package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class FindingAidsTest {

	SolrFieldGenerator gen = new FindingAids();

	@Test
	public void testIndexes() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"555",' ',' ',"‡a Vols. 1-21, 1976-88. 1 v."));
		String expected =
		"indexes_display: Vols. 1-21, 1976-88. 1 v.\n"+
		"notes_t: Vols. 1-21, 1976-88. 1 v.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testFindingAids() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"555",'0',' ',"‡a Box list."));
		String expected =
		"finding_aids_display: Box list.\n"+
		"notes_t: Box list.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testNotes() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"555",'8',' ',"‡a Includes indexes."));
		String expected =
		"notes: Includes indexes.\n"+
		"notes_t: Includes indexes.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testTwoFields() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"555",' ',' ',
				"‡a INDEXES: v.1-5 (1966-1970); INDEXES: v.6-10 (1971-1973);"));
		rec.dataFields.add(new DataField(2,"555",' ',' ',
				"‡a INDEXES: v.11-15 (1973-1975); INDEXES: v.16-20 (1976-1978)"));
		String expected =
		"indexes_display: INDEXES: v.1-5 (1966-1970); INDEXES: v.6-10 (1971-1973);\n"+
		"notes_t: INDEXES: v.1-5 (1966-1970); INDEXES: v.6-10 (1971-1973);\n"+
		"indexes_display: INDEXES: v.11-15 (1973-1975); INDEXES: v.16-20 (1976-1978)\n"+
		"notes_t: INDEXES: v.11-15 (1973-1975); INDEXES: v.16-20 (1976-1978)\n";
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void test880() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(3, 4,"555",' ',' ',
				"‡6 880-04 ‡a Dai 1-shū dai 1-gō (Nov. 1914)-dai 2-shū dai 5-gō (June 1916)",false));
		rec.dataFields.add(new DataField(17,4,"555",' ',' ',
				"‡6 555-04/$1 ‡a 第1集第1号 (Nov. 1914)-第2集第5号-(June 1916)",true));
		String expected =
		"indexes_display: 第1集第1号 (Nov. 1914)-第2集第5号-(June 1916)\n"+
		"notes_t_cjk: 第1集第1号 (Nov. 1914)-第2集第5号-(June 1916)\n"+
		"indexes_display: Dai 1-shū dai 1-gō (Nov. 1914)-dai 2-shū dai 5-gō (June 1916)\n"+
		"notes_t: Dai 1-shū dai 1-gō (Nov. 1914)-dai 2-shū dai 5-gō (June 1916)\n";
//		System.out.println(FindingAids.generateSolrFields(rec,null).toString().replaceAll("\"","\\\\\""));
		assertEquals(expected,this.gen.generateSolrFields(rec,null).toString());
	}
}
