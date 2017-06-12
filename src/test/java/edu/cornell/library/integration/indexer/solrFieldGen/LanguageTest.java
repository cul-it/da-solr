package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;

@SuppressWarnings("static-method")
public class LanguageTest {

	@Test
	public void test008() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"830222c19771975cau      b    001 0 eng d"));
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,        vals.facet.size());
		assertEquals("English",vals.facet.iterator().next());
		assertEquals(1,        vals.display.size());
		assertEquals("English",vals.display.iterator().next());
		assertEquals(0,        vals.notes.size());
	}

	@Test
	public void test008Chinese() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"170202s2017    ch a          000 0 chi  "));
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,        vals.facet.size());
		assertEquals("Chinese",vals.facet.iterator().next());
		assertEquals(1,        vals.display.size());
		assertEquals("Chinese",vals.display.iterator().next());
		assertEquals(0,        vals.notes.size());
	}

	@Test
	public void testLanguageNote() {
		DataField f = new DataField(3,"546");
		f.subfields.add(new Subfield(1, 'a', "Free text language note"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(0,vals.facet.size());
		assertEquals(1,vals.notes.size());
		assertEquals("Free text language note",vals.notes.iterator().next());
	}

	@Test
	public void test008WithNote() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"830222c19771975cau      b    001 0 eng d"));
		DataField f = new DataField(3,"546");
		f.subfields.add(new Subfield(1, 'a', "In English"));
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,           vals.facet.size());
		assertEquals("English",   vals.facet.iterator().next());
		assertEquals(0,           vals.display.size());
		assertEquals(1,           vals.notes.size());
		assertEquals("In English",vals.notes.iterator().next());
	}

	@Test
	public void test041a() {
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "spa"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,        vals.facet.size());
		assertEquals("Spanish",vals.facet.iterator().next());
		assertEquals(1,        vals.display.size());
		assertEquals("Spanish",vals.display.iterator().next());
		assertEquals(0,        vals.notes.size());
	}

	@Test
	public void test008With041a() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"070529s2017    vm a   e      000 0 vie d"));
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "vie"));
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,           vals.facet.size());
		assertEquals("Vietnamese",vals.facet.iterator().next());
		assertEquals(1,           vals.display.size());
		assertEquals("Vietnamese",vals.display.iterator().next());
		assertEquals(0,           vals.notes.size());
	}

	@Test
	public void testAllThree() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"161212s2016    ii 158            vlhin d"));
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "hin"));
		f.subfields.add(new Subfield(2, 'j', "eng"));
		f.subfields.add(new Subfield(3, 'h', "hin"));
		rec.dataFields.add(f);
		f = new DataField(4,"546");
		f.subfields.add(new Subfield(1, 'a', "In Hindi with English subtitles."));
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(2,               vals.facet.size());
		assertEquals("Hindi, English",String.join(", ",vals.facet));
		assertEquals(0,               vals.display.size());
		assertEquals(1,               vals.notes.size());
		assertEquals("In Hindi with English subtitles.",vals.notes.iterator().next());
	}

	@Test
	public void testSubfieldFiltering() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "hin")); // display & facet
		f.subfields.add(new Subfield(2, 'b', "eng")); // display only
		f.subfields.add(new Subfield(3, 'h', "spa")); // neither
		rec.dataFields.add(f);
		Language.SolrFieldValueSet vals = Language.generateSolrFields ( rec );
		assertEquals(1,               vals.facet.size());
		assertEquals("Hindi",         String.join(", ",vals.facet));
		assertEquals(2,               vals.display.size());
		assertEquals("Hindi, English",String.join(", ",vals.display));
		assertEquals(0,               vals.notes.size());
	}
}