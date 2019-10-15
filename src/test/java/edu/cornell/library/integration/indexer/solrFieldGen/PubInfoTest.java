package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;


public class PubInfoTest {

	SolrFieldGenerator gen = new PubInfo();

	@Test
	public void test9586284() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008","160630s2014    oruab   ob   f000 0 eng c"));
		rec.dataFields.add(new DataField(3,"264",' ','1',"‡a Eugene, Ore. : ‡b University of Oregon, ‡c 2014."));
		rec.dataFields.add(new DataField(4,"264",' ','2',"‡a Omaha, Neb. : ‡b National Park Service"));
		String expected =
		"pub_date_sort: 2014\n"+
		"pub_date_facet: 2014\n"+
		"pub_info_display: Eugene, Ore. : University of Oregon, 2014.\n"+
		"pubplace_display: Eugene, Ore.\n"+
		"pubplace_t: Eugene, Ore.\n"+
		"publisher_display: University of Oregon\n"+
		"publisher_t: University of Oregon\n"+
		"pub_dist_display: Omaha, Neb. : National Park Service\n"+
		"pubplace_display: Omaha, Neb.\n"+
		"pubplace_t: Omaha, Neb.\n"+
		"publisher_display: National Park Service\n"+
		"publisher_t: National Park Service\n"+
		"pub_date_display: 2014\n"+
		"pub_date_t:     \n"+
		"pub_date_t: 2014\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void test8631644() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.controlFields.add(new ControlField(1,"008","140902p20142013njuuunn           n zxx d"));
		rec.dataFields.add(new DataField(3,"264",' ','1',
				"‡a [Jersey City, New Jersey] : ‡b Erstwhile Records, ‡c [2014]"));
		rec.dataFields.add(new DataField(4,"264",' ','4',"‡c ℗2014, ‡c ©2014"));
		String expected =
		"pub_date_sort: 2013\n"+
		"pub_date_facet: 2013\n"+
		"pub_info_display: [Jersey City, New Jersey] : Erstwhile Records, [2014]\n"+
		"pubplace_display: [Jersey City, New Jersey]\n"+
		"pubplace_t: [Jersey City, New Jersey]\n"+
		"publisher_display: Erstwhile Records\n"+
		"publisher_t: Erstwhile Records\n"+
		"pub_copy_display: ℗2014, ©2014\n"+
		"pub_date_display: 2014\n"+
		"pub_date_t: 2013\n"+
		"pub_date_t: 2014\n"+
		"pub_date_t: [2014]\n"+
		"pub_date_t: ©2014\n"+
		"pub_date_t: ℗2014,\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void test5073103Without008OrMain260() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,3,"260",' ',' ',"‡6 260-03/$1 ‡a 東京 : ‡b 吉川弘文館, ‡c 2004.",true));
//		System.out.println(gen.generateSolrFields(rec,null).toString().replaceAll("\"", "\\\\\""));
		String expected =
		"pub_info_display: 東京 : 吉川弘文館, 2004.\n"+
		"pubplace_t_cjk: 東京 :\n"+
		"publisher_t_cjk: 吉川弘文館,\n"+
		"pubplace_display: 東京\n"+
		"pubplace_t: 東京\n"+
		"publisher_display: 吉川弘文館\n"+
		"publisher_t: 吉川弘文館\n"+
		"pub_date_display: 2004\n"+
		"pub_date_t: 2004\n";
		assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
	}

	@Test
	public void testRPDates_missingSecondDate() throws ClassNotFoundException, SQLException, IOException {
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.id = "10310563";
			rec.controlFields.add(new ControlField(1,"008","130613r1864||||enk     o     ||1 0|eng|d"));
			String expected =
			"pub_date_sort: 1864\n" + 
			"pub_date_facet: 1864\n" + 
			"pub_date_t: 1864\n" + 
			"pub_date_t: ||||\n";
			assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
		}
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.id = "717506";
			rec.controlFields.add(new ControlField(1,"008","781009r196016uuit            000 0 lat d"));
			String expected =
			"pub_date_sort: 1600\n" + 
			"pub_date_facet: 1600\n" + 
			"pub_date_t: 16uu\n" + 
			"pub_date_t: 1960\n";
			assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
		}
	}

	@Test
	public void testCompletelyMissing008dates() throws ClassNotFoundException, SQLException, IOException {
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.controlFields.add(new ControlField(1,"008","130613s||||||||enk     o     ||1 0|eng|d"));
			String expected = "pub_date_t: ||||\n";
			assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
		}
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.controlFields.add(new ControlField(1,"008","130613p||||||||enk     o     ||1 0|eng|d"));
			String expected = "pub_date_t: ||||\n";
			assertEquals( expected, this.gen.generateSolrFields(rec,null).toString());
		}
	}
}
