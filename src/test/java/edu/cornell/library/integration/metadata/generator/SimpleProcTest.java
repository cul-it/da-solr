package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class SimpleProcTest {

	SolrFieldGenerator gen = new SimpleProc();

	@Test
	public void testNoNotes() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		assertEquals("",this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test520() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"520",' ',' ',"‡a Here's a 520 note."));
		String expected =
		"summary_display: Here's a 520 note.\n"+
		"notes_t: Here's a 520 note.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test581() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC); //b9691888
		rec.dataFields.add(new DataField(1,"581",' ',' ',
				"‡a Murmurs of Earth : the Voyager interstellar record /"
				+ " Carl Sagan [and others]. 1st ed. New York : Random House, ©1978. ‡z 9780394410470"));
		String expected =
		"works_about_display: Murmurs of Earth : the Voyager interstellar record /"
		+ " Carl Sagan [and others]. 1st ed. New York : Random House, ©1978. 9780394410470\n" + 
		"notes_t: Murmurs of Earth : the Voyager interstellar record /"
		+ " Carl Sagan [and others]. 1st ed. New York : Random House, ©1978.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());

		rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC); //b2082571
		rec.dataFields.add(new DataField(1,"581",' ',' ',"‡3 Letter ‡a published in WHAT THEY WROTE by Carol Kammen."));
		expected =
		"works_about_display: Letter published in WHAT THEY WROTE by Carol Kammen.\n" + 
		"notes_t: published in WHAT THEY WROTE by Carol Kammen.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test541() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"541",'1',' ',"‡a Here's a 541 note."));
		String expected = "donor_display: Here's a 541 note.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}
	@Test
	public void testRestricted541() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"541",'0',' ',"‡a Here's a restricted 541 note."));
		assertEquals("",this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test300() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"300",' ',' ',"‡a Here's a 300 note."));
		String expected =
		"description_display: Here's a 300 note.\n"+
		"notes_t: Here's a 300 note.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
		rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 178 p. : ‡b ill. (some col.) ; ‡c 24 cm + ‡e 1 CD-ROM."));
		expected =
		"description_display: 178 p. : ill. (some col.) ; 24 cm + 1 CD-ROM.\n" + 
		"notes_t: 178 p. : ill. (some col.) ; 24 cm + 1 CD-ROM.\n" + 
		"f300e_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testLccn() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"010",' ',' ',"‡a 2015231566"));
		String expected =
		"lc_controlnum_display: 2015231566\n"+
		"lc_controlnum_s: 2015231566\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testEdition() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"250",' ',' ',"‡a First edition."));
		String expected = 
		"edition_display: First edition.\n"+
		"notes_t: First edition.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testVariousOtherTitles() throws ClassNotFoundException, SQLException, IOException { //117081
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
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void test222LeadingArticle() throws ClassNotFoundException, SQLException, IOException { //117081
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"222",'0','3',"‡a La Rassegna della letteratura italiana"));
		String expected =
		"title_addl_t: La Rassegna della letteratura italiana\n"+
		"title_addl_t: Rassegna della letteratura italiana\n";
//		System.out.println(gen.generateSolrFields(rec, null).toString());
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testTwoNotes() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"500",' ',' ',"‡a Here's the first note."));
		rec.dataFields.add(new DataField(2,"500",' ',' ',"‡a Here's the second note."));
		String expected =
		"notes: Here's the first note.\n"+
		"notes_t: Here's the first note.\n"+
		"notes: Here's the second note.\n"+
		"notes_t: Here's the second note.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testNonRomanNote() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,1,"500",' ',' ',"‡6 880-01 ‡a Here's the main note.", false));
		rec.dataFields.add(new DataField(2,1,"500",' ',' ',
				"‡6 500-01/$1 ‡a Here's the non-Roman version of the note.", true));
		String expected =
		"notes: Here's the non-Roman version of the note.\n"+
		"notes_t_cjk: Here's the non-Roman version of the note.\n"+
		"notes: Here's the main note.\n"+
		"notes_t: Here's the main note.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testComplex() throws ClassNotFoundException, SQLException, IOException {
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
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void twoDonor902Field() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"902",' ',' ',"‡a pfnd ‡b Goodkind ‡b Midland Friends"));
		rec.id = "7461";
		assertEquals(
				"donor_display: Goodkind\n" + 
				"donor_display: Midland Friends\n",
				this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testAward() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 66 pages ; ‡c 20 cm"));
		rec.dataFields.add(new DataField(2,"520",' ',' ',"‡a \"The story of Jackself is ..."));
		rec.dataFields.add(new DataField(3,"586",' ',' ',"‡a T. S. Eliot Prize for Poetry, 2016."));
		rec.id = "9909446";
		String expected =
		"description_display: 66 pages ; 20 cm\n" + 
		"notes_t: 66 pages ; 20 cm\n" + 
		"summary_display: \"The story of Jackself is ...\n" + 
		"notes_t: \"The story of Jackself is ...\n" + 
		"awards_display: T. S. Eliot Prize for Poetry, 2016.\n" + 
		"notes_t: T. S. Eliot Prize for Poetry, 2016.\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void participantPerformerNotes() throws ClassNotFoundException, SQLException, IOException {
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"511",'1',' ',
					"‡a Orson Welles, Joseph Cotten, Agnes Moorehead, Everett Sloane."));
			String expected =
			"cast_display: Orson Welles, Joseph Cotten, Agnes Moorehead, Everett Sloane.\n" + 
			"notes_t: Orson Welles, Joseph Cotten, Agnes Moorehead, Everett Sloane.\n";
			assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
		}
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"511",'0',' ',
					"‡a Fires of London ; the composer conducting."));
			String expected =
			"notes: Fires of London ; the composer conducting.\n" + 
			"notes_t: Fires of London ; the composer conducting.\n";
			assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
		}
	}

	@Test
	public void conditional773() throws ClassNotFoundException, SQLException, IOException {
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"773",'0',' ',"‡t EBSCO Publication Finder ‡d EBSCO ‡o 1861142"));
			assertEquals("notes_t: EBSCO Publication Finder EBSCO 1861142\n",
					this.gen.generateSolrFields(rec, null).toString());
		}
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"773",'0',' ',
					"‡a Morse, Jedidiah, 1761-1826. ‡t American geography. ‡b New ed., rev., cor., and greatly enlarged."
					+ " ‡d London : Printed for John Stockdale, 1794. ‡g [Map 6], facing p. 284. ‡w (OCoLC)51834027"));
			String expected =
			"in_display: Morse, Jedidiah, 1761-1826. American geography. New ed., rev., cor., and greatly enlarged."
			+ " London : Printed for John Stockdale, 1794. [Map 6], facing p. 284. (OCoLC)51834027\n"+
			"notes_t: Morse, Jedidiah, 1761-1826. American geography. New ed., rev., cor., and greatly enlarged."
			+ " London : Printed for John Stockdale, 1794. [Map 6], facing p. 284. (OCoLC)51834027\n";
			assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
		}
	}
}
