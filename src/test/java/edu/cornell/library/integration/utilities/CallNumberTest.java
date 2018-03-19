package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class CallNumberTest {

	static SolrBuildConfig config = null;
	SolrFieldGenerator gen = new CallNumber();

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("CallNos");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
	}

	@Test
	public void testNoCallNo() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		assertEquals("",gen.generateSolrFields(rec,config).toString());
	}

	@Test
	public void testEmptyCallNo() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'0',' ',"‡h"));
		bibRec.holdings.add(holdingRec);
		assertEquals("",gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testHoldingCallNo() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'0',' ',"‡h QA611 ‡i .R123.6"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testHoldingCallNoWithPrefix() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'0',' ',"‡k Thesis ‡h QA611 ‡i .R123.6"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Thesis QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}
	@Test
	public void testHoldingCallNoThesisInSubfieldH() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'0',' ',"‡h Thesis QA611 ‡i .R123.6"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: Thesis QA611 .R123.6\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testNonLCHoldingCallNo() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'0',' ',"‡h Film 1-0-3"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: Film 1-0-3\n"+
		"callnum_sort: Film 1-0-3\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testBib050CallNo() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testSortSelectionBetweenThreeHoldings() throws ClassNotFoundException, SQLException, IOException {
		// Between call numbers from holdings, the one that is LC should be sorted.
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'1',' ',"‡h Video 7"));
		holdingRec.dataFields.add(new DataField(2,"852",'0',' ',"‡h QA611 ‡i .R123.6"));
		holdingRec.dataFields.add(new DataField(3,"852",'1',' ',"‡h Video 8"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: Video 7\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Video 8\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testSortSelectionBetweenBibCallnos() throws ClassNotFoundException, SQLException, IOException {
		// Between two call numbers from bibs, the one that is LC should be sorted.
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(1,"050",'1',' ',"‡a Video 7"));
		bibRec.dataFields.add(new DataField(2,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		bibRec.dataFields.add(new DataField(3,"950",'1',' ',"‡a Video 8"));
		String expected =
		"lc_callnum_full: Video 7\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Video 8\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}

	@Test
	public void testSortSelectionBetweenBibAndHoldings() throws ClassNotFoundException, SQLException, IOException {
		// The call number holdings should be preferred for sort even if non-LC 
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.dataFields.add(new DataField(2,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",'1',' ',"‡h Video 7"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Video 7\n"+
		"callnum_sort: Video 7\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
//		System.out.println( CallNumber.generateSolrFields(bibRec,config).toString() );
		assertEquals(expected,gen.generateSolrFields(bibRec,config).toString());
	}
}
