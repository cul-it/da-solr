package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;

public class FormatTest {

	SolrFieldGenerator gen = new Format();

	@BeforeClass
	public static void instantiateTestInstanceResourceTypes() throws IOException {
		SupportReferenceData.initializeInstanceTypes("example_reference_data/instance-types.json");
		SupportReferenceData.initializeLocations("example_reference_data/locations.json");
	}

	@Test
	public void testBook() throws SQLException, IOException {
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
		rec.marcHoldings.add(hRec);
		String expected =
		"format: Book\n"+
		"format_main_facet: Book\n"+
		"bib_format_display: am\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testSerial() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9467170";
		rec.leader = "01326cas a22003132u 4500";
		rec.controlFields.add(new ControlField(1,"008","160609q18uu20uuxx || |so||||||   ||und|d"));
		rec.dataFields.add(new DataField(2,"245",'1','0',"‡a Sociology."));
		rec.dataFields.add(new DataField(3,"948",'1',' ',"‡a 20160613 ‡b s ‡d batch ‡e lts ‡f j"));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "9797953";
		hRec.dataFields.add(new DataField(1,"852",'8',' ',"‡b serv,remo ‡h No call number"));
		rec.marcHoldings.add(hRec);
		String expected =
		"format: Journal/Periodical\n"+
		"format_main_facet: Journal/Periodical\n"+
		"bib_format_display: as\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testMicroformThesisMusicalScore() throws SQLException, IOException {
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
		rec.marcHoldings.add(hRec);
		String expected =
		"format: Thesis\n"+
		"format: Microform\n"+
		"format: Musical Score\n"+
		"format_main_facet: Musical Score\n"+
		"bib_format_display: cm\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testWebsite() throws SQLException, IOException {
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
		rec.marcHoldings.add(hRec);
		String expected =
		"format: Website\n"+
		"format_main_facet: Website\n"+
		"bib_format_display: ai\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testDatabase() throws SQLException, IOException {
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
		"bib_format_display: ai\n"+
		"database_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testManuscript() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9707628";
		rec.leader = "03054atm a2200481 i 4500";
		rec.controlFields.add(new ControlField(1,"008","151228t20162016enka     b    001 0 eng d"));
		rec.dataFields.add(new DataField(3,"245",'1','0',"‡a Angels in early medieval England / ‡c Richard Sowerby."));
		String expected =
		"format: Manuscript/Archive\n"+
		"format_main_facet: Manuscript/Archive\n"+
		"bib_format_display: tm\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testArchive() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9709512";
		rec.leader = "01548cpcaa2200265 a 4500";
		rec.controlFields.add(new ControlField(1,"008","161122s2016    nyu                 eng d"));
		rec.dataFields.add(new DataField(3,"245",'0','0',
				"‡a Cornell University School of Chemical Engineering records, ‡f 2016."));
		MarcRecord hRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		hRec.id = "3232234";
		hRec.dataFields.add(new DataField(1,"852",'0','0',"‡b rmc ‡k Archives ‡h 16-7-4244"));
		rec.marcHoldings.add(hRec);
		String expected =
		"format: Manuscript/Archive\n"+
		"format_main_facet: Manuscript/Archive\n"+
		"bib_format_display: pc\n"+
		"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}

	@Test
	public void testInstanceResourceTypes() throws IOException {
		Map<String,Object> instance = new LinkedHashMap<>();

		instance.put("instanceTypeId", "c7f7446f-4642-4d97-88c9-55bae2ad6c7f");
		assertEquals(
		"format: Non-musical Recording\n" + 
		"format_main_facet: Non-musical Recording\n" + 
		"database_b: false\n",
		this.gen.generateNonMarcSolrFields(instance, null).toString());

		instance.put("instanceTypeId", "2afc8005-8654-4401-8321-d991f8cb95e9");
		assertEquals(
		"format: Book\n" + 
		"format_main_facet: Book\n" + 
		"database_b: false\n",
		this.gen.generateNonMarcSolrFields(instance, null).toString());

		instance.put("instanceTypeId", "6312d172-f0cf-40f6-b27d-9fa8feaf332f");
		assertEquals(
		"format: Book\n" + 
		"format_main_facet: Book\n" + 
		"database_b: false\n",
		this.gen.generateNonMarcSolrFields(instance, null).toString());

		instance.put("instanceTypeId", "497b5090-3da2-486c-b57f-de5bb3c2e26d");
		assertEquals(
		"format: Musical Score\n" + 
		"format_main_facet: Musical Score\n" + 
		"database_b: false\n",
		this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void testLocations() throws SQLException, IOException {
		SupportReferenceData.initializeLocations("example_reference_data/locations.json");

		// online, o
		MarcRecord rec = generateTestMarcRecord("01548cocaa2200265 a 4500", "serv,remo");
		String expected =
				"format: Kit\n"+
				"format_main_facet: Kit\n"+
				"bib_format_display: oc\n"+
				"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());

		// rare anex, o
		rec = generateTestMarcRecord("01548cocaa2200265 a 4500", "asia,ranx");
		expected =
				"format: Kit\n"+
				"format_main_facet: Kit\n"+
				"bib_format_display: oc\n"+
				"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());

		// online, p
		rec = generateTestMarcRecord("01548cpcaa2200265 a 4500", "serv,remo");
		expected =
				"format: Miscellaneous\n"+
				"format_main_facet: Miscellaneous\n"+
				"bib_format_display: pc\n"+
				"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());

		// rare anex, p
		rec = generateTestMarcRecord("01548cpcaa2200265 a 4500", "asia,ranx");
		expected =
				"format: Manuscript/Archive\n"+
				"format_main_facet: Manuscript/Archive\n"+
				"bib_format_display: pc\n"+
				"database_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, null).toString());
	}
	
	public MarcRecord generateTestMarcRecord(String leader, String locationId) {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "9709512";
		rec.leader = leader;
		rec.controlFields.add(new ControlField(1,"008","161122s2016    nyu                 eng d"));
		rec.dataFields.add(new DataField(3,"245",'0','0',
				"‡a Cornell University School of Chemical Engineering records, ‡f 2016."));

		String locId = SupportReferenceData.locations.getUuid(locationId);
		Map<String,Object> folioHolding = new HashMap<>();
		folioHolding.put("permanentLocationId", locId);
		List<Map<String,Object>> folioHoldingList = new ArrayList<>();
		folioHoldingList.add(folioHolding);
		rec.folioHoldings = folioHoldingList;
		
		return rec;
	}
}
