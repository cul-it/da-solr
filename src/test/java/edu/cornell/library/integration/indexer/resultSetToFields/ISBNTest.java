package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class ISBNTest {

	@Test
	public void testOldStyle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 (pbk.)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testOldStyleC() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 9782709656825 (pbk.) : ‡c 19,00 EUR"));
		String expected =
		"isbn_t: 9782709656825\n"+
		"isbn_display: 9782709656825 (pbk.)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testOldStyle880() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, 1, "020", ' ',' ',"‡6 880-01 ‡a 4892032867 (v. 2)", false));
		rec.dataFields.add(new DataField( 2, 1, "020", ' ',' ',"‡6 020-01/$1 ‡a 4892032867 (中卷)", true));
		String expected =
		"isbn_t: 4892032867\n"+
		"isbn_display: 4892032867 (中卷)\n"+
		"isbn_t: 4892032867\n"+
		"isbn_display: 4892032867 (v. 2)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q (pbk.)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}
	@Test
	public void testNewerStyleZ() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡z 12344567 ‡q (pbk.)"));
		assertEquals( "", ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle2q() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q (pbk.; ‡q ebook)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk. ; ebook)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle880() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, 1, "020", ' ',' ',"‡6 880-01 ‡a 9789860433265", false));
		rec.dataFields.add(new DataField( 2, 1, "020", ' ',' ',"‡6 020-01/$1 ‡a 9789860433265 ‡q (平裝)", true));
		String expected =
		"isbn_t: 9789860433265\n"+
		"isbn_display: 9789860433265 (平裝)\n"+
		"isbn_t: 9789860433265\n"+
		"isbn_display: 9789860433265\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewestStyle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q pbk."));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewestStyle2q() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q pbk. ‡q ebook"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk. ; ebook)\n";
//		System.out.println( ISBN.generateSolrFields ( rec, null ).toString().replaceAll("\"","\\\\\"") );
		assertEquals( expected, ISBN.generateSolrFields ( rec, null ).toString());
	}
}
