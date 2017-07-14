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
		String expected =
		"language_facet: English\n"+
		"language_display: English\n"+
		"language_articles_t: the a an\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void test008Chinese() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"170202s2017    ch a          000 0 chi  "));
		String expected =
		"language_facet: Chinese\n"+
		"language_display: Chinese\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void testLanguageNote() {
		DataField f = new DataField(3,"546");
		f.subfields.add(new Subfield(1, 'a', "Free text language note"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(f);
		String expected = "language_display: Free text language note\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void test008WithNote() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"830222c19771975cau      b    001 0 eng d"));
		DataField f = new DataField(3,"546");
		f.subfields.add(new Subfield(1, 'a', "In English."));
		rec.dataFields.add(f);
		String expected =
		"language_facet: English\n"+
		"language_display: In English.\n"+
		"language_articles_t: the a an\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void test041a() {
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "spa"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(f);
		String expected =
		"language_facet: Spanish\n"+
		"language_display: Spanish\n"+
		"language_articles_t: el la lo los las un una unos unas\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void test008With041a() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008",
				"070529s2017    vm a   e      000 0 vie d"));
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "vie"));
		rec.dataFields.add(f);
		String expected =
		"language_facet: Vietnamese\n"+
		"language_display: Vietnamese\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
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
		String expected =
		"language_facet: Hindi\n"+
		"language_facet: English\n"+
		"language_display: In Hindi with English subtitles.\n"+
		"language_articles_t: the a an\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}

	@Test
	public void testSubfieldFiltering() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(3,"041");
		f.subfields.add(new Subfield(1, 'a', "hin")); // display & facet
		f.subfields.add(new Subfield(2, 'b', "eng")); // display only
		f.subfields.add(new Subfield(3, 'h', "spa")); // neither
		rec.dataFields.add(f);
		String expected =
		"language_facet: Hindi\n"+
		"language_display: Hindi, English\n"+
		"language_articles_t: the a an\n"+
		"language_articles_t: el la lo los las un una unos unas\n";
		assertEquals(expected,Language.generateSolrFields ( rec ).toString());
	}
}
