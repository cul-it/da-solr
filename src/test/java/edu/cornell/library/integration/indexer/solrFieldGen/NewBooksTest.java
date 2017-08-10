package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Tests for edu.cornell.library.integration.indexer.solrFieldGen.NewBooks.java.<br/>
 * <b>Because some of the behavior of the NewBooks.java involves comparing the dates in records with
 * the current date, the expected results will eventually change and the tests will require updating.</b>
 */
public class NewBooksTest {

	SolrFieldGenerator gen = new NewBooks();

	@Test
	public void testAfricanaNew() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',"‡a 20170720 ‡b f ‡d fw11 ‡e lts"));
		bibRec.id = "9953401";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "10268578";
		holdingRec.controlFields.add(new ControlField(1,"005","20170724130511.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b afr ‡h E169.Z83 ‡i P48 2017 ‡z Temporarily shelved on the New Books Shelf."));
		bibRec.holdings.add(holdingRec);
		String expected =
		"new_shelf: Africana Library New Books Shelf\n"+
		"acquired: 20170720\n"+
		"acquired_month: 201707\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testOlinNewNoteworthy() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20170426 ‡b f ‡d sir28 ‡e lts"));
		bibRec.id = "9901078";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "10219193";
		holdingRec.controlFields.add(new ControlField(1,"005","20170427164010.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b olin ‡k New & Noteworthy Books ‡h PS3601.R542 ‡i A6 2017"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"new_shelf: Olin Library New & Noteworthy Books\n"+
		"acquired: 20170426\n"+
		"acquired_month: 201704\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testTransferToAnnex() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20040706 ‡b f ‡d mann11 ‡e mann ‡f ? ‡h ?"));
		bibRec.id = "5068025";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "5591697";
		holdingRec.controlFields.add(new ControlField(1,"005","20170509104901.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b mann ‡h QL737.M336 ‡i O83x 2003 ‡x transfer from Mann Ellis 5/9/17"));
		bibRec.holdings.add(holdingRec);
		assertEquals( "", gen.generateSolrFields(bibRec, null).toString() );
	}
}