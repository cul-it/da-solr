package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;

/**
 * Tests for edu.cornell.library.integration.indexer.solrFieldGen.NewBooks.java.<br/>
 * <b>Because some of the behavior of the NewBooks.java involves comparing the dates in records with
 * the current date, the test record dates have been pushed forward by 1000 years, ensuring the
 * dates will never pass the important "more than two years old" threshold beyond which acquisition
 * dates are no longer indexed.</b>
 */
public class NewBooksTest {

	SolrFieldGenerator gen = new NewBooks();

	@Test
	public void testAfricanaNew() throws SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',"‡a 30170720 ‡b f ‡d fw11 ‡e lts"));
		bibRec.id = "9953401";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "10268578";
		holdingRec.controlFields.add(new ControlField(1,"005","30170724130511.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b afr ‡h E169.Z83 ‡i P48 2017 ‡z Temporarily shelved on the New Books Shelf."));
		bibRec.marcHoldings.add(holdingRec);
		String expected =
		"new_shelf: Africana Library New Books Shelf\n"+
		"acquired_dt: 3017-07-20T00:00:00Z\n"+
		"acquired_month: 3017-07\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testOlinNewNoteworthy() throws SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 30170426 ‡b f ‡d sir28 ‡e lts"));
		bibRec.id = "9901078";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "10219193";
		holdingRec.controlFields.add(new ControlField(1,"005","30170427164010.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b olin ‡k New & Noteworthy Books ‡h PS3601.R542 ‡i A6 2017"));
		bibRec.marcHoldings.add(holdingRec);
		String expected =
		"new_shelf: Olin Library New & Noteworthy Books\n"+
		"acquired_dt: 3017-04-26T00:00:00Z\n"+
		"acquired_month: 3017-04\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testTransferToAnnex() throws SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',
				"‡a 20040706 ‡b f ‡d mann11 ‡e mann ‡f ? ‡h ?"));
		bibRec.id = "5068025";
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "5591697";
		holdingRec.controlFields.add(new ControlField(1,"005","20170509104901.0"));
		holdingRec.dataFields.add(new DataField(2,"852",'0','0',
				"‡b mann ‡h QL737.M336 ‡i O83x 2003 ‡x transfer from Mann Ellis 5/9/17"));
		bibRec.marcHoldings.add(holdingRec);
		assertEquals( "acquired_dt: 2004-07-06T00:00:00Z\nacquired_month: 2004-07\n",
				this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testInvalidAcquisitionDate() throws SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"948",'1',' ',"‡a 20022904 ‡b l ‡d pem2 ‡e lts ‡f ? ‡h ?"));
		bibRec.id = "520808";
		assertEquals( "acquired_date_invalid_b: true\n",
				this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testFolioHoldings() throws SQLException, IOException {
		String resourceDataJson = loadResourceFile("example_reference_data/locations.json");
		SupportReferenceData.initializeLocations(resourceDataJson);
		String instanceStatusesJson = loadResourceFile("example_reference_data/instance_statuses.json");
		SupportReferenceData.initializeInstanceStatuses(instanceStatusesJson);
		String statusIdBatch = SupportReferenceData.instanceStatuses.getUuid("batch");
		String statusIdCataloged = SupportReferenceData.instanceStatuses.getUuid("cat");

		// not online, batch
		MarcRecord bibRec = generateTestMarcRecord("asia,ranx", statusIdBatch, "3022-06-17T18:56:21.660+00:00");
		String expected = "";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );

		// online, created date earlier than golive
		bibRec = generateTestMarcRecord("serv,remo", statusIdCataloged, "1000-06-17T18:56:21.660+00:00");
		expected = "";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );

		// online, craeted date after golive
		bibRec = generateTestMarcRecord("serv,remo", statusIdBatch, "3022-06-17T18:56:21.660+00:00");
		expected =
				"acquired_dt: 3022-06-17T14:56:21Z\n"+
				"acquired_month: 3022-06\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}
	
	public MarcRecord generateTestMarcRecord(String locationId, String statusId, String createdDate) {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9953401";
		String locId = SupportReferenceData.locations.getUuid(locationId);
		Map<String,Object> folioHolding = new HashMap<>();
		folioHolding.put("permanentLocationId", locId);
		List<Map<String,Object>> folioHoldingList = new ArrayList<>();
		folioHoldingList.add(folioHolding);
		rec.folioHoldings = folioHoldingList;
		Map<String, Object> instance = new HashMap<String, Object>();
		rec.instance = instance;
		instance.put("statusId", statusId);
		Map<String, String> meta = new HashMap<>();
		rec.instance.put("metadata", meta);
		meta.put("createdDate", createdDate);

		return rec;
	}
	
	public String loadResourceFile(String filename) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}
}
