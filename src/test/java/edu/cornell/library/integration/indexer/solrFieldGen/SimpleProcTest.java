package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class SimpleProcTest {

	@Test
	public void testNoNotes() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		assertEquals("",SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test520() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"520",' ',' ',"‡a Here's a 520 note."));
		String expected =
		"summary_display: Here's a 520 note.\n"+
		"notes_t: Here's a 520 note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test541() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"541",'1',' ',"‡a Here's a 541 note."));
		String expected = "donor_display: Here's a 541 note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}
	@Test
	public void testRestricted541() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"541",'0',' ',"‡a Here's a restricted 541 note."));
		assertEquals("",SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test300() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"300",' ',' ',"‡a Here's a 300 note."));
		String expected = "description_display: Here's a 300 note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testLccn() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"010",' ',' ',"‡a 2015231566"));
		String expected =
		"lc_controlnum_display: 2015231566\n"+
		"lc_controlnum_s: 2015231566\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test035() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)924835975"));
		String expected =
		"other_id_display: (OCoLC)924835975\n"+
		"id_t: (OCoLC)924835975\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testEdition() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"250",' ',' ',"‡a First edition."));
		String expected = "edition_display: First edition.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testVariousOtherTitles() { //117081
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"210",'0',' ',"‡a J.S. Afr. Vet. Assoc."));
		rec.dataFields.add(new DataField(2,"210",'1','0',"‡a J S Afr Vet Assoc ‡2 dnlm"));
		rec.dataFields.add(new DataField(3,"222",' ','0',"‡a Journal of the South African Veterinary Association"));
		rec.dataFields.add(new DataField(4,"246",'3','1',"‡a Tydskrif van die Suid-Afrikaanse Veterinêre Vereniging"));
		rec.dataFields.add(new DataField(5,"246",'1',' ',"‡a South African Veterinary Association journal"));
		String expected =
		"title_addl_t: J.S. Afr. Vet. Assoc.\n"+
		"title_addl_t: J S Afr Vet Assoc\n"+
		"title_addl_t: Journal of the South African Veterinary Association\n"+
		"title_addl_t: Journal of the South African Veterinary Association\n"+
		"title_other_display: Tydskrif van die Suid-Afrikaanse Veterinêre Vereniging\n"+
		"title_addl_t: Tydskrif van die Suid-Afrikaanse Veterinêre Vereniging\n"+
		"title_other_display: South African Veterinary Association journal\n"+
		"title_addl_t: South African Veterinary Association journal\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test222LeadingArticle() { //117081
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"222",'0','3',"‡a La Rassegna della letteratura italiana"));
		String expected =
		"title_addl_t: La Rassegna della letteratura italiana\n"+
		"title_addl_t: Rassegna della letteratura italiana\n";
//		System.out.println(SimpleProc.generateSolrFields(rec, null).toString());
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testTwoNotes() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"500",' ',' ',"‡a Here's the first note."));
		rec.dataFields.add(new DataField(2,"500",' ',' ',"‡a Here's the second note."));
		String expected =
		"notes: Here's the first note.\n"+
		"notes_t: Here's the first note.\n"+
		"notes: Here's the second note.\n"+
		"notes_t: Here's the second note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testNonRomanNote() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,1,"500",' ',' ',"‡6 880-01 ‡a Here's the main note.", false));
		rec.dataFields.add(new DataField(2,1,"500",' ',' ',
				"‡6 500-01/$1 ‡a Here's the non-Roman version of the note.", true));
		String expected =
		"notes: Here's the non-Roman version of the note.\n"+
		"notes_t_cjk: Here's the non-Roman version of the note.\n"+
		"notes: Here's the main note.\n"+
		"notes_t: Here's the main note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testComplex() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"500",' ',' ',"‡a Here's the first note."));
		rec.dataFields.add(new DataField(2,1,"500",' ',' ',
				"‡6 880-01 ‡a Here's the second note with non-Roman version.", false));
		rec.dataFields.add(new DataField(3,"500",' ',' ',"‡3 Context note: ‡a Here's the third note."));
		rec.dataFields.add(new DataField(4,1,"500",' ',' ',
				"‡6 500-01/$1 ‡a Here's the non-Roman version of the second note.", true));
		String expected =
		"notes: Here's the first note.\n"+
		"notes_t: Here's the first note.\n"+
		"notes: Here's the non-Roman version of the second note.\n"+
		"notes_t_cjk: Here's the non-Roman version of the second note.\n"+
		"notes: Here's the second note with non-Roman version.\n"+
		"notes_t: Here's the second note with non-Roman version.\n"+
		"notes: Context note: Here's the third note.\n"+
		"notes_t: Here's the third note.\n";
		assertEquals(expected,SimpleProc.generateSolrFields(rec, null).toString());
	}
}
