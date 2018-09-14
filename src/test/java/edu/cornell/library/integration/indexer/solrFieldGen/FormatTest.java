package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class FormatTest {

	SolrFieldGenerator gen = new Format();

	@Test
	public void testBook() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9811567";
		rec.leader = "02339cam a2200541 i 4500";
		rec.controlFields.add(new ControlField(1,"008","170208t20182018maua    o     001 0 eng d"));
		rec.dataFields.add(new DataField(2,"245",'1','0',
				"‡a Advanced engineering mathematics / ‡c Dennis G. Zill, Loyola Marymount University."));
		rec.dataFields.add(new DataField(3,"948",'1',' ',"‡a 20170214 ‡b s ‡d batch ‡e lts ‡f ebk"));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "10132842";
		hRec.dataFields.add(new DataField(1,"852",'8',' ',"‡b serv,remo ‡h No call number"));
		rec.holdings.add(hRec);
		String expected =
		"format: Book\n"+
		"format_main_facet: Book\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testSerial() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9467170";
		rec.leader = "01326cas a22003132u 4500";
		rec.controlFields.add(new ControlField(1,"008","160609q18uu20uuxx || |so||||||   ||und|d"));
		rec.dataFields.add(new DataField(2,"245",'1','0',"‡a Sociology."));
		rec.dataFields.add(new DataField(3,"948",'1',' ',"‡a 20160613 ‡b s ‡d batch ‡e lts ‡f j"));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "9797953";
		hRec.dataFields.add(new DataField(1,"852",'8',' ',"‡b serv,remo ‡h No call number"));
		rec.holdings.add(hRec);
		String expected =
		"format: Journal/Periodical\n"+
		"format_main_facet: Journal/Periodical\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testMicroformThesisMusicalScore() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "2370518";
		rec.leader = "00929ccm a22003011 4500";
		rec.controlFields.add(new ControlField(1,"007","hdrauu---bucu"));
		rec.controlFields.add(new ControlField(2,"008","930805s1962    miuzza  a      n        d"));
		rec.dataFields.add(new DataField(3,"245",'1','0',"‡a Symmetries for orchestra ‡h [microfilm]"));
		rec.dataFields.add(new DataField(4,"502",' ',' ',"‡a Thesis--University of Minnesota, 1961."));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "2842392";
		hRec.dataFields.add(new DataField(1,"852",'8',' ',"‡b mus ‡h Film 15"));
		rec.holdings.add(hRec);
		String expected =
		"format: Thesis\n"+
		"format: Microform\n"+
		"format: Musical Score\n"+
		"format_main_facet: Musical Score\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testWebsite() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "2719878";
		rec.leader = "02266cai a2200505 a 4500";
		rec.controlFields.add(new ControlField(1,"008","951006c20049999nyukr wss    f0   a2eng d"));
		rec.dataFields.add(new DataField(3,"245",'0','0',
				"‡a Ag census ‡h [electronic resource] : ‡b the U.S. census of agriculture, 1987, 1992, 1997."));
		rec.dataFields.add(new DataField(4,"653",' ',' ',"‡a Agriculture"));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "3232234";
		hRec.dataFields.add(new DataField(1,"852",'0','0',"‡b serv,remo ‡k ONLINE ‡h HD1753 1992 ‡i .F3"));
		rec.holdings.add(hRec);
		String expected =
		"format: Website\n"+
		"format_main_facet: Website\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testDatabase() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "2138527";
		rec.leader = "03110cai a2200697 a 4500";
		rec.controlFields.add(new ControlField(1,"008","890427c19719999miuwr dssai   0    2eng d"));
		rec.dataFields.add(new DataField(2,"245",'0','0',"‡a ABI/Inform ‡h [electronic resource]."));
		rec.dataFields.add(new DataField(3,"653",' ',' ',"‡a Business and Management"));
		rec.dataFields.add(new DataField(4,"653",' ',' ',"‡a Economics"));
		rec.dataFields.add(new DataField(5,"653",' ',' ',"‡a Human Resources, Labor & Employment (Core)"));
		rec.dataFields.add(new DataField(6,"653",' ',' ',"‡a Social Sciences"));
		rec.dataFields.add(new DataField(7,"948",'1',' ',"‡a 19990909 ‡b s ‡d batch ‡e lts ‡f fd"));
		rec.dataFields.add(new DataField(8,"948",'2',' ',"‡a 20071010 ‡b m ‡d pjs4 ‡e lts ‡f webfeatdb"));
		String expected =
		"format: Database\n"+
		"format_main_facet: Database\n"+
		"database_b: true\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testManuscript() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9707628";
		rec.leader = "03054atm a2200481 i 4500";
		rec.controlFields.add(new ControlField(1,"008","151228t20162016enka     b    001 0 eng d"));
		rec.dataFields.add(new DataField(3,"245",'1','0',"‡a Angels in early medieval England / ‡c Richard Sowerby."));
		String expected =
		"format: Manuscript/Archive\n"+
		"format_main_facet: Manuscript/Archive\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testArchive() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9709512";
		rec.leader = "01548cpcaa2200265 a 4500";
		rec.controlFields.add(new ControlField(1,"008","161122s2016    nyu                 eng d"));
		rec.dataFields.add(new DataField(3,"245",'0','0',
				"‡a Cornell University School of Chemical Engineering records, ‡f 2016."));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "3232234";
		hRec.dataFields.add(new DataField(1,"852",'0','0',"‡b rmc ‡k Archives ‡h 16-7-4244"));
		rec.holdings.add(hRec);
		String expected =
		"format: Manuscript/Archive\n"+
		"format_main_facet: Manuscript/Archive\n"+
		"database_b: false\n";
		assertEquals(expected,gen.generateSolrFields(rec, null).toString());
	}
}
