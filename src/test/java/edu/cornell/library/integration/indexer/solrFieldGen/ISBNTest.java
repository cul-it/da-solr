package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class ISBNTest {

	SolrFieldGenerator gen = new ISBN();

	@Test
	public void testOldStyle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 (pbk.)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testOldStyleC() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 9782709656825 (pbk.) : ‡c 19,00 EUR"));
		String expected =
		"isbn_t: 9782709656825\n"+
		"isbn_display: 9782709656825 (pbk.)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testOldStyleColonSeparatedSubfieldQs() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 9789712726187 ‡q (book 3 : ‡q np)"));
		String expected =
		"isbn_t: 9789712726187\n"+
		"isbn_display: 9789712726187 (book 3 ; np)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testOldStyle880() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, 1, "020", ' ',' ',"‡6 880-01 ‡a 4892032867 (v. 2)", false));
		rec.dataFields.add(new DataField( 2, 1, "020", ' ',' ',"‡6 020-01/$1 ‡a 4892032867 (中卷)", true));
		String expected =
		"isbn_t: 4892032867\n"+
		"isbn_display: 4892032867 (中卷)\n"+
		"isbn_t: 4892032867\n"+
		"isbn_display: 4892032867 (v. 2)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q (pbk.)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}
	@Test
	public void testNewerStyleZ() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡z 12344567 ‡q (pbk.)"));
		assertEquals( "", this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle2q() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q (pbk.; ‡q ebook)"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk. ; ebook)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewerStyle880() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, 1, "020", ' ',' ',"‡6 880-01 ‡a 9789860433265", false));
		rec.dataFields.add(new DataField( 2, 1, "020", ' ',' ',"‡6 020-01/$1 ‡a 9789860433265 ‡q (平裝)", true));
		String expected =
		"isbn_t: 9789860433265\n"+
		"isbn_display: 9789860433265 (平裝)\n"+
		"isbn_t: 9789860433265\n"+
		"isbn_display: 9789860433265\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewestStyle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q pbk."));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk.)\n";
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}

	@Test
	public void testNewestStyle2q() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField( 1, "020", ' ',' ',"‡a 12344567 ‡q pbk. ‡q ebook"));
		String expected =
		"isbn_t: 12344567\n"+
		"isbn_display: 12344567 (pbk. ; ebook)\n";
//		System.out.println( ISBN.generateSolrFields ( rec, null ).toString().replaceAll("\"","\\\\\"") );
		assertEquals( expected, this.gen.generateSolrFields ( rec, null ).toString());
	}
}
