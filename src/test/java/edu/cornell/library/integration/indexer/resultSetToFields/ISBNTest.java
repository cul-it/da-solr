package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

@SuppressWarnings("static-method")
public class ISBNTest {

	@Test
	public void testOldStyle() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "12344567 (pbk.)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk.)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}

	@Test
	public void testOldStyleC() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.id = 1;
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "9782709656825 (pbk.) :"));
		f.subfields.add(new MarcRecord.Subfield(2, 'c', "19,00 EUR"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.iterator().next().equals("9782709656825 (pbk.)"));
		assertTrue(vals.searchMain.iterator().next().equals("9782709656825"));
	}

	@Test
	public void testOldStyle880() {
		MarcRecord.DataField f1 = new MarcRecord.DataField();
		f1.id = 1;
		f1.tag = "020";
		f1.subfields.add(new MarcRecord.Subfield(1, 'a', "4892032867 (v. 2)"));
		MarcRecord.DataField f2 = new MarcRecord.DataField();
		f2.id = 2;
		f2.tag = "880";
		f2.subfields.add(new MarcRecord.Subfield(1, '6', "020-00/$1"));
		f2.subfields.add(new MarcRecord.Subfield(2, 'a', "4892032867 (中卷)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f1));
		fs.fields.add(new FieldSet.FSDataField(f2));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.iterator().next().equals("4892032867 (中卷)"));
		assertTrue(vals.search880.iterator().next().equals("4892032867"));
		assertTrue(vals.displayMain.iterator().next().equals("4892032867 (v. 2)"));
		assertTrue(vals.searchMain.iterator().next().equals("4892032867"));
	}

	@Test
	public void testNewerStyle() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "12344567"));
		f.subfields.add(new MarcRecord.Subfield(2, 'q', "(pbk.)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk.)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}

	@Test
	public void testNewerStyleZ() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'z', "12344567"));
		f.subfields.add(new MarcRecord.Subfield(2, 'q', "(pbk.)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.isEmpty());
		assertTrue(vals.searchMain.isEmpty());
	}

	@Test
	public void testNewerStyle2q() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "12344567"));
		f.subfields.add(new MarcRecord.Subfield(2, 'q', "(pbk.;"));
		f.subfields.add(new MarcRecord.Subfield(3, 'q', "ebook)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk. ; ebook)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}

	@Test
	public void testNewerStyle880() {
		MarcRecord.DataField f1 = new MarcRecord.DataField();
		f1.id = 1;
		f1.tag = "020";
		f1.subfields.add(new MarcRecord.Subfield(1, '6', "880-01"));
		f1.subfields.add(new MarcRecord.Subfield(2, 'a', "9789860433265"));
		MarcRecord.DataField f2 = new MarcRecord.DataField();
		f2.id = 2;
		f2.tag = "880";
		f2.alttag = "020";
		f2.subfields.add(new MarcRecord.Subfield(1, '6', "020-01/$1"));
		f2.subfields.add(new MarcRecord.Subfield(2, 'a', "9789860433265"));
		f2.subfields.add(new MarcRecord.Subfield(3, 'q', "(平裝)"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f1));
		fs.fields.add(new FieldSet.FSDataField(f2));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.iterator().next().equals("9789860433265 (平裝)"));
		assertTrue(vals.search880.iterator().next().equals("9789860433265"));
		assertTrue(vals.displayMain.iterator().next().equals("9789860433265"));
		assertTrue(vals.searchMain.iterator().next().equals("9789860433265"));
	}

	@Test
	public void testNewestStyle() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "12344567"));
		f.subfields.add(new MarcRecord.Subfield(2, 'q', "pbk."));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk.)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}

	@Test
	public void testNewestStyle2q() {
		MarcRecord.DataField f = new MarcRecord.DataField();
		f.tag = "020";
		f.subfields.add(new MarcRecord.Subfield(1, 'a', "12344567"));
		f.subfields.add(new MarcRecord.Subfield(2, 'q', "pbk."));
		f.subfields.add(new MarcRecord.Subfield(3, 'q', "ebook"));
		MarcRecord.FieldSet fs = new MarcRecord.FieldSet();
		fs.fields.add(new FieldSet.FSDataField(f));
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk. ; ebook)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}
}
