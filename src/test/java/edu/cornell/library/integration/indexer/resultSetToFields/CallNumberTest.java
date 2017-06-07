package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.CallNumber.Sort;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.Subfield;

@SuppressWarnings("static-method")
public class CallNumberTest {

	static SolrBuildConfig config = null;
	static CallNumber callno = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("CallNos");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
		callno = new CallNumber();
	}

	@Test
	public void testNoCallNo() throws ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(0,    vals.search.size());
			assertEquals(0,    vals.facet.size());
			assertEquals(null, vals.sort);
		}
	}

	@Test
	public void testEmptyCallNo() throws ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		DataField f = new DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'h', ""));
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(0,    vals.search.size());
			assertEquals(0,    vals.facet.size());
			assertEquals(null, vals.sort);
		}
	}

	@Test
	public void testHoldingCallNo() throws ClassNotFoundException, SQLException {
		DataField f = new DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'h', "QA611"));
		f.subfields.add(new Subfield(2, 'i', ".R123.6"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(1,              vals.search.size());
			assertEquals("QA611 .R123.6",vals.search.iterator().next());
			assertEquals("QA611 .R123.6",vals.sort.sortVal);
			assertEquals(3,              vals.facet.size());
			assertTrue(vals.facet.contains(
					"Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology"));
		}
	}

	@Test
	public void testHoldingCallNoWithPrefix() throws ClassNotFoundException, SQLException {
		DataField f = new DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'k', "Thesis"));
		f.subfields.add(new Subfield(2, 'h', "QA611"));
		f.subfields.add(new Subfield(3, 'i', ".R123.6"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(2,              vals.search.size());
			assertTrue(vals.search.contains("Thesis QA611 .R123.6"));
			assertTrue(vals.search.contains("QA611 .R123.6"));
			assertEquals("QA611 .R123.6",vals.sort.sortVal);
			assertEquals(3,              vals.facet.size());
			assertTrue(vals.facet.contains(
					"Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology"));
		}
	}

	@Test
	public void testHoldingCallNoThesisInSubfieldH() throws ClassNotFoundException, SQLException {
		DataField f = new DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'h', "Thesis QA611"));
		f.subfields.add(new Subfield(2, 'i', ".R123.6"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(2,              vals.search.size());
			assertTrue(vals.search.contains("Thesis QA611 .R123.6"));
			assertTrue(vals.search.contains("QA611 .R123.6"));
			assertEquals("QA611 .R123.6",vals.sort.sortVal);
			assertEquals(3,              vals.facet.size());
			assertTrue(vals.facet.contains(
					"Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology"));
		}
	}
	@Test
	public void testNonLCHoldingCallNo() throws ClassNotFoundException, SQLException {
		DataField f = new DataField(3,"852");
		f.ind1 = '1';
		f.subfields.add(new Subfield(1, 'h', "Film 1-0-3"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(1,           vals.search.size());
			assertEquals("Film 1-0-3",vals.search.iterator().next());
			assertEquals("Film 1-0-3",vals.sort.sortVal);
			assertEquals(0,           vals.facet.size());
		}
	}

	@Test
	public void testBib050CallNo() throws ClassNotFoundException, SQLException {
		DataField f = new DataField(3,"050");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'a', "QA611"));
		f.subfields.add(new Subfield(2, 'b', ".R123.6"));
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(f);
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			assertEquals(1,              vals.search.size());
			assertEquals("QA611 .R123.6",vals.search.iterator().next());
			assertEquals("QA611 .R123.6",vals.sort.sortVal);
			assertEquals(3,              vals.facet.size());
			assertTrue(vals.facet.contains(
					"Q - Science:QA - Mathematics:QA440-699 - Geometry.  Trigonometry.  Topology"));
		}
	}

	@Test
	public void testSortSelectionBetweenTwoHoldings() throws ClassNotFoundException, SQLException {
		// Between two call numbers from holdings, the one that is LC should be sorted.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		DataField f = new DataField(3,"852");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'h', "QA611"));
		f.subfields.add(new Subfield(2, 'i', ".R123.6"));
		rec.dataFields.add(f);
		f = new DataField(4,"852");
		f.ind1 = '1';
		f.subfields.add(new Subfield(1, 'h', "Video 8"));
		rec.dataFields.add(f);
		List<Sort> sorts = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			if (vals.sort != null)
				sorts.add(vals.sort);
		}
		assertEquals("QA611 .R123.6",CallNumber.chooseSortValue(sorts));
	}

	@Test
	public void testSortSelectionBetweenTwoBibs() throws ClassNotFoundException, SQLException {
		// Between two call numbers from bibs, the one that is LC should be sorted.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(3,"050");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'a', "QA611"));
		f.subfields.add(new Subfield(2, 'b', ".R123.6"));
		rec.dataFields.add(f);
		f = new DataField(4,"950");
		f.ind1 = '1';
		f.subfields.add(new Subfield(1, 'a', "Video 8"));
		rec.dataFields.add(f);
		List<Sort> sorts = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			if (vals.sort != null)
				sorts.add(vals.sort);
		}
		assertEquals("QA611 .R123.6",CallNumber.chooseSortValue(sorts));
	}

	@Test
	public void testSortSelectionBetweenBibAndHoldings() throws ClassNotFoundException, SQLException {
		// The call number holdings should be preferred for sort even if non-LC 
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		DataField f = new DataField(3,"050");
		f.ind1 = '0';
		f.subfields.add(new Subfield(1, 'a', "QA611"));
		f.subfields.add(new Subfield(2, 'b', ".R123.6"));
		rec.dataFields.add(f);
		f = new DataField(4,"852");
		f.ind1 = '1';
		f.subfields.add(new Subfield(1, 'h', "Video 8"));
		rec.dataFields.add(f);
		List<Sort> sorts = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			CallNumber.SolrFieldValueSet vals = callno.generateSolrFields(fs,config);
			if (vals.sort != null)
				sorts.add(vals.sort);
		}
		assertEquals("Video 8",CallNumber.chooseSortValue(sorts));
	}
}
