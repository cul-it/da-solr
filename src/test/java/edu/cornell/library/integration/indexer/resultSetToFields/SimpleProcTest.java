package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.Subfield;

@SuppressWarnings("static-method")
public class SimpleProcTest {

	@Test
	public void testNoNotes() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals(null,vals.displayField);
		}
	}

	@Test
	public void test520() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"520");
		f.subfields.add(new Subfield(1, 'a', "Here's a 520 note."));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("summary_display",vals.displayField);
			assertEquals("notes_t",vals.searchField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(1,vals.searchValues.size());
			assertEquals("Here's a 520 note.",vals.displayValues.get(0));
			assertEquals("Here's a 520 note.",vals.searchValues.get(0));
		}
	}

	@Test
	public void test541() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"541");
		f.ind1 = '1';
		f.subfields.add(new Subfield(1, 'a', "Here's a 541 note."));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("donor_display",vals.displayField);
			assertEquals("notes_t",vals.searchField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(0,vals.searchValues.size());
			assertEquals("Here's a 541 note.",vals.displayValues.get(0));
		}
	}
	@Test
	public void testRestricted541() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"541");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'a', "Here's a restricted 541 note."));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals(0,vals.displayValues.size());
			assertEquals(0,vals.searchValues.size());
		}
	}

	@Test
	public void test300() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"300");
		f.subfields.add(new Subfield(1, 'a', "Here's a 300 note."));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("description_display",vals.displayField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(0,vals.searchValues.size());
			assertEquals("Here's a 300 note.",vals.displayValues.get(0));
		}
	}

	@Test
	public void testLccn() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"010");
		f.subfields.add(new Subfield(1, 'a', "2015231566"));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("lc_controlnum_display",vals.displayField);
			assertEquals("lc_controlnum_s",vals.searchField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(1,vals.searchValues.size());
			assertEquals("2015231566",vals.displayValues.get(0));
			assertEquals("2015231566",vals.searchValues.get(0));
		}
	}

	@Test
	public void test035() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"035");
		f.subfields.add(new Subfield(1, 'a', "(OCoLC)924835975"));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("other_id_display",vals.displayField);
			assertEquals("id_t",vals.searchField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(1,vals.searchValues.size());
			assertEquals("(OCoLC)924835975",vals.displayValues.get(0));
			assertEquals("(OCoLC)924835975",vals.searchValues.get(0));
		}
	}

	@Test
	public void testEdition() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(1,"250");
		f.subfields.add(new Subfield(1, 'a', "First edition."));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("edition_display",vals.displayField);
			assertEquals(1,vals.displayValues.size());
			assertEquals(0,vals.searchValues.size());
			assertEquals("First edition.",vals.displayValues.get(0));
		}
	}

	@Test
	public void testTwoNotes() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(2,"500");
		f.subfields.add(new Subfield(1, 'a', "Here's the second note."));
		rec.dataFields.add(f);
		f = new DataField(1,"500");
		f.subfields.add(new Subfield(1, 'a', "Here's the first note."));
		rec.dataFields.add(f);
		List<String> displays = new ArrayList<>();
		List<String> searches = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("notes",vals.displayField);
			assertEquals("notes_t",vals.searchField);
			displays.addAll(vals.displayValues);
			searches.addAll(vals.searchValues);
		}
		assertEquals(2,displays.size());
		assertEquals("Here's the first note.",displays.get(0));
		assertEquals("Here's the second note.",displays.get(1));
		assertEquals(2,searches.size());
		assertEquals("Here's the first note.",searches.get(0));
		assertEquals("Here's the second note.",searches.get(1));
	}

	@Test
	public void testNonRomanNote() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f;
		f = new DataField(1,1,"500");
		f.subfields.add(new Subfield(1, '6', "880-01"));
		f.subfields.add(new Subfield(2, 'a', "Here's the main note."));
		rec.dataFields.add(f);
		f = new DataField(2,1,"500",true);
		f.subfields.add(new Subfield(1, '6', "500-01"));
		f.subfields.add(new Subfield(2, 'a', "Here's the non-Roman version of the note."));
		rec.dataFields.add(f);
		List<String> displays = new ArrayList<>();
		List<String> searches = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("notes",vals.displayField);
			assertEquals("notes_t",vals.searchField);
			displays.addAll(vals.displayValues);
			searches.addAll(vals.searchValues);
		}
		assertEquals(2,displays.size());
		assertEquals("Here's the non-Roman version of the note.",displays.get(0));
		assertEquals("Here's the main note.",displays.get(1));
		assertEquals(2,searches.size());
		assertEquals("Here's the non-Roman version of the note.",searches.get(0));
		assertEquals("Here's the main note.",searches.get(1));
	}

	@Test
	public void testComplex() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f;
		f = new DataField(1,"515");
		f.subfields.add(new Subfield(1, 'a', "Here's the first note."));
		rec.dataFields.add(f);
		f = new DataField(2,1,"500");
		f.subfields.add(new Subfield(1, '6', "880-01"));
		f.subfields.add(new Subfield(2, 'a', "Here's the second note with non-Roman version."));
		rec.dataFields.add(f);
		f = new DataField(3,"556");
		f.subfields.add(new Subfield(1, '3', "Context note:"));
		f.subfields.add(new Subfield(2, 'a', "Here's the third note."));
		rec.dataFields.add(f);
		f = new DataField(6,1,"500",true);
		f.subfields.add(new Subfield(1, '6', "500-01"));
		f.subfields.add(new Subfield(2, 'a', "Here's the non-Roman version of the note."));
		rec.dataFields.add(f);
		List<String> displays = new ArrayList<>();
		List<String> searches = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			SimpleProc.SolrFieldValueSet vals = SimpleProc.generateSolrFields(fs);
			assertEquals("notes",vals.displayField);
			assertEquals("notes_t",vals.searchField);
			displays.addAll(vals.displayValues);
			searches.addAll(vals.searchValues);
		}
		assertEquals(4,displays.size());
		assertEquals("Here's the first note.",displays.get(0));
		assertEquals("Here's the non-Roman version of the note.",displays.get(1));
		assertEquals("Here's the second note with non-Roman version.",displays.get(2));
		assertEquals("Context note: Here's the third note.",displays.get(3));
		assertEquals(4,searches.size());
		assertEquals("Here's the first note.",searches.get(0));
		assertEquals("Here's the non-Roman version of the note.",searches.get(1));
		assertEquals("Here's the second note with non-Roman version.",searches.get(2));
		assertEquals("Here's the third note.",searches.get(3));
	}
}
