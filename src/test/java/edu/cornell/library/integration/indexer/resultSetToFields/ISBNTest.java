package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.Subfield;

@SuppressWarnings("static-method")
public class ISBNTest {

	@Test
	public void testOldStyle() {
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "12344567 (pbk.)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
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
		DataField f = new DataField();
		f.id = 1;
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "9782709656825 (pbk.) :"));
		f.subfields.add(new Subfield(2, 'c', "19,00 EUR"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.iterator().next().equals("9782709656825 (pbk.)"));
		assertTrue(vals.searchMain.iterator().next().equals("9782709656825"));
	}

	@Test
	public void testOldStyle880() {
		DataField f1 = new DataField();
		f1.id = 1;
		f1.tag = "020";
		f1.subfields.add(new Subfield(1, 'a', "4892032867 (v. 2)"));
		DataField f2 = new DataField();
		f2.id = 2;
		f2.tag = "880";
		f2.subfields.add(new Subfield(1, '6', "020-00/$1"));
		f2.subfields.add(new Subfield(2, 'a', "4892032867 (中卷)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020")
				.addToFields(f1).addToFields(f2).build();
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.iterator().next().equals("4892032867 (中卷)"));
		assertTrue(vals.search880.iterator().next().equals("4892032867"));
		assertTrue(vals.displayMain.iterator().next().equals("4892032867 (v. 2)"));
		assertTrue(vals.searchMain.iterator().next().equals("4892032867"));
	}

	@Test
	public void testNewerStyle() {
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "12344567"));
		f.subfields.add(new Subfield(2, 'q', "(pbk.)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
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
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'z', "12344567"));
		f.subfields.add(new Subfield(2, 'q', "(pbk.)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.isEmpty());
		assertTrue(vals.searchMain.isEmpty());
	}

	@Test
	public void testNewerStyle2q() {
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "12344567"));
		f.subfields.add(new Subfield(2, 'q', "(pbk.;"));
		f.subfields.add(new Subfield(3, 'q', "ebook)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
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
		DataField f1 = new DataField();
		f1.id = 1;
		f1.tag = "020";
		f1.subfields.add(new Subfield(1, '6', "880-01"));
		f1.subfields.add(new Subfield(2, 'a', "9789860433265"));
		DataField f2 = new DataField();
		f2.id = 2;
		f2.tag = "880";
		f2.alttag = "020";
		f2.subfields.add(new Subfield(1, '6', "020-01/$1"));
		f2.subfields.add(new Subfield(2, 'a', "9789860433265"));
		f2.subfields.add(new Subfield(3, 'q', "(平裝)"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020")
				.addToFields(f1).addToFields(f2).build();
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.iterator().next().equals("9789860433265 (平裝)"));
		assertTrue(vals.search880.iterator().next().equals("9789860433265"));
		assertTrue(vals.displayMain.iterator().next().equals("9789860433265"));
		assertTrue(vals.searchMain.iterator().next().equals("9789860433265"));
	}

	@Test
	public void testNewestStyle() {
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "12344567"));
		f.subfields.add(new Subfield(2, 'q', "pbk."));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
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
		DataField f = new DataField();
		f.tag = "020";
		f.subfields.add(new Subfield(1, 'a', "12344567"));
		f.subfields.add(new Subfield(2, 'q', "pbk."));
		f.subfields.add(new Subfield(3, 'q', "ebook"));
		DataFieldSet fs = new DataFieldSet.Builder().setId(1).setMainTag("020").addToFields(f).build();
		ISBN.SolrFieldValueSet vals = ISBN.generateSolrFields ( fs );
		assertTrue(vals.display880.isEmpty());
		assertTrue(vals.search880.isEmpty());
		assertTrue(vals.displayMain.size() == 1);
		assertTrue(vals.searchMain.size() == 1);
		assertTrue(vals.displayMain.iterator().next().equals("12344567 (pbk. ; ebook)"));
		assertTrue(vals.searchMain.iterator().next().equals("12344567"));
	}
}
