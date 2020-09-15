package edu.cornell.library.integration.utilities;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.metadata.support.CallNumber;

public class CallNumberTest {

	static Config config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = Config.getRequiredArgsForDB("CallNos");
		config = Config.loadConfig(requiredArgs);
	}

	@Test
	public void testNoCallNo() throws SQLException {
		CallNumber cn = new CallNumber();
		assertEquals("",cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testEmptyCallNo() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'0',' ',"‡h"));
		assertEquals("",cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testHoldingCallNo() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'0',' ',"‡h QA611 ‡i .R123.6"));
//		System.out.println(cn.summarizeCallNumbers(config).toString());
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testHoldingCallNoWithPrefix() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'0',' ',"‡k Thesis ‡h QA611 ‡i .R123.6"));
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Thesis QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testHoldingCallNoThesisInSubfieldH() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'0',' ',"‡h Thesis QA611 ‡i .R123.6"));
		String expected =
		"lc_callnum_full: Thesis QA611 .R123.6\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testNonLCHoldingCallNo() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'0',' ',"‡h Film 1-0-3"));
		String expected =
		"lc_callnum_full: Film 1-0-3\n"+
		"callnum_sort: Film 1-0-3\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testBib050CallNo() throws SQLException {
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_bib_display: QA611 .R123.6\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testSortSelectionBetweenThreeHoldings() throws SQLException {
		// Between call numbers from holdings, the one that is LC should be sorted.
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"852",'1',' ',"‡h Video 7"));
		cn.tabulateCallNumber(new DataField(2,"852",'0',' ',"‡h QA611 ‡i .R123.6"));
		cn.tabulateCallNumber(new DataField(3,"852",'1',' ',"‡h Video 8"));
		String expected =
		"lc_callnum_full: Video 7\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_callnum_full: Video 8\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testSortSelectionBetweenBibCallnos() throws SQLException {
		// Between two call numbers from bibs, the one that is LC should be sorted.
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(1,"050",'1',' ',"‡a Video 7"));
		cn.tabulateCallNumber(new DataField(2,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		cn.tabulateCallNumber(new DataField(3,"950",'1',' ',"‡a Video 8"));
		String expected =
		"lc_callnum_full: Video 7\n"+
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_bib_display: QA611 .R123.6\n"+
		"lc_callnum_full: Video 8\n"+
		"callnum_sort: QA611 .R123.6\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}

	@Test
	public void testSortSelectionBetweenBibAndHoldings() throws SQLException {
		// The call number holdings should be preferred for sort even if non-LC 
		CallNumber cn = new CallNumber();
		cn.tabulateCallNumber(new DataField(2,"050",'0',' ',"‡a QA611 ‡b .R123.6"));
		cn.tabulateCallNumber(new DataField(1,"852",'1',' ',"‡h Video 7"));
		String expected =
		"lc_callnum_full: QA611 .R123.6\n"+
		"lc_bib_display: QA611 .R123.6\n"+
		"lc_callnum_full: Video 7\n"+
		"callnum_sort: Video 7\n"+
		"lc_callnum_facet: Q - Science\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics\n"+
		"lc_callnum_facet: Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology\n";
		assertEquals(expected,cn.getCallNumberFields(config).toString());
	}
}
