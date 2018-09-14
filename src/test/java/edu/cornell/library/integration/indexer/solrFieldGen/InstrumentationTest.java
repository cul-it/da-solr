package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class InstrumentationTest {

	SolrFieldGenerator gen = new Instrumentation();

	@Test
	public void test8894788() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',
				"‡a soprano voice ‡n 1 ‡a violin ‡n 2 ‡a viola ‡n 1 ‡a cello ‡n 1 ‡s 5 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: For soprano voice; violin (2); viola; cello. Total performers: 5\n"+
		"notes_t: For soprano voice; violin (2); viola; cello. Total performers: 5\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void test8856631() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',
				"‡a soprano voice ‡n 1 ‡a clarinet ‡n 1 ‡a violin ‡n 1 ‡a cello ‡n 1 ‡a double bass ‡n 1 "
				+ "‡a piano ‡n 1 ‡s 6 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: For soprano voice; clarinet; violin; cello; double bass; piano. Total performers: 6\n"+
		"notes_t: For soprano voice; clarinet; violin; cello; double bass; piano. Total performers: 6\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void test9264398() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',"‡a mixed chorus ‡e 1 ‡v SATB ‡a orchestra ‡e 1 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: For mixed chorus [SATB]; orchestra\n"+
		"notes_t: For mixed chorus [SATB]; orchestra\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}	

	@Test
	public void test8745671() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',"‡a flute ‡d piccolo ‡n 1 ‡a oboe ‡n 1 ‡a clarinet ‡v doubles"
				+ " on E♭ clarinet ‡n 1 ‡a bassoon ‡n 1 ‡a horn ‡n 1 ‡a piano ‡n 1 ‡a violin ‡n 2 ‡a viola ‡n 1 ‡a"
				+ " cello ‡n 1 ‡a double bass ‡n 1 ‡s 11 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: For flute/piccolo; oboe; clarinet [doubles on E♭ clarinet]; bassoon; horn; piano;"
		+ " violin (2); viola; cello; double bass. Total performers: 11\n"+
		"notes_t: For flute/piccolo; oboe; clarinet [doubles on E♭ clarinet]; bassoon; horn; piano; violin (2); viola;"
		+ " cello; double bass. Total performers: 11\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void test9394028() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',
				"‡a mixed chorus ‡e 1 ‡a organ ‡n 1 ‡p piano ‡n 1 ‡a bowed string ensemble ‡e 1 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: For mixed chorus; organ or piano; bowed string ensemble\n"+
		"notes_t: For mixed chorus; organ or piano; bowed string ensemble\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}

	@Test
	public void test9301693() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"382",'0','1',"‡v First work ‡a percussion ensemble ‡s 4 ‡2 lcmpt"));
		rec.dataFields.add(new DataField(2,"382",'0','1',
				"‡v Second work ‡a percussion ensemble ‡a piano ‡n 2 ‡s 6 ‡2 lcmpt"));
		rec.dataFields.add(new DataField(3,"382",'0','1',"‡v Third work ‡a percussion ensemble ‡s 2 ‡2 lcmpt"));
		rec.dataFields.add(new DataField(4,"382",'0','1',"‡v Fourth work ‡a percussion ensemble ‡s 5 ‡2 lcmpt"));
		String expected =
		"instrumentation_display: [First work]: For percussion ensemble. Total performers: 4\n"+
		"notes_t: [First work]: For percussion ensemble. Total performers: 4\n"+
		"instrumentation_display: [Second work]: For percussion ensemble; piano (2). Total performers: 6\n"+
		"notes_t: [Second work]: For percussion ensemble; piano (2). Total performers: 6\n"+
		"instrumentation_display: [Third work]: For percussion ensemble. Total performers: 2\n"+
		"notes_t: [Third work]: For percussion ensemble. Total performers: 2\n"+
		"instrumentation_display: [Fourth work]: For percussion ensemble. Total performers: 5\n"+
		"notes_t: [Fourth work]: For percussion ensemble. Total performers: 5\n";
		assertEquals( expected , gen.generateSolrFields(rec,null).toString() );
	}
}
