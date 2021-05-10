package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class HoldingsAndItemsTest {

	static Config config = null;
	SolrFieldGenerator gen = new HoldingsAndItems();

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		config = Config.loadConfig(requiredArgs);
	}

	@Test
	public void testOnlineHoldings()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10121881";
		holdRec.modifiedDate = "20170214132422.0";
		holdRec.leader = "00183nx  a22000851  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10121881"));
		holdRec.controlFields.add(new ControlField(2,"004","9800604"));
		holdRec.controlFields.add(new ControlField(3,"005","20170214132422.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1702140u||||8|||4001uu|||0000000"));
		holdRec.dataFields.add(new DataField(5,"852",'8',' ',"‡b serv,remo ‡h No call number"));
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "02020cam a22004574a 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 1 online resource."));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10121881|20170214132422\n"+
		"multivol_b: false\n";
		assertEquals(expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testPrintHolding()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10091152";
		holdRec.modifiedDate = "20170321083807.0";
		holdRec.leader = "00183nx  a22000851  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10091152"));
		holdRec.controlFields.add(new ControlField(2,"004","9769380"));
		holdRec.controlFields.add(new ControlField(3,"005","20170321083807.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1701310u||||8|||4001uu|||0000000"));
		holdRec.dataFields.add(new DataField(5,"852",'8',' ',"‡b ech ‡h DS665 ‡i .C36 2016"));
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "01167cam a2200349Mi 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 205 p. ; ‡c 21 cm."));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10091152|20170321083807\n"+
		"lc_callnum_full: DS665 .C36 2016\n"+
		"callnum_sort: DS665 .C36 2016\n"+
		"item_display: 10165353|10091152\n"+
		"location_facet: Kroch Library Asia\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}
	
	public void testMultipleCopies()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "06409cam a2200601 i 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',
				"‡a xxxii, 346 pages : ‡b illustrations (chiefly color) ; ‡c 27 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10155075";
		holdRec.modifiedDate = "20170306131913.0";
		holdRec.leader = "00183nx  a22000851  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10155075"));
		holdRec.controlFields.add(new ControlField(2,"004","8855036"));
		holdRec.controlFields.add(new ControlField(3,"005","20170306131913.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1703012u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(5,"852",'8',' ',"‡b mann ‡t 2 ‡h TD878.48 ‡i .K46 2015"));
		bibRec.holdings.add(holdRec);
		holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10155076";
		holdRec.modifiedDate = "20170306132457.0";
		holdRec.leader = "00186cx  a22000851  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10155076"));
		holdRec.controlFields.add(new ControlField(2,"004","8855036"));
		holdRec.controlFields.add(new ControlField(3,"005","20170306132457.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1703012u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(5,"852",'8',' ',"‡b mann ‡t 3 ‡h TD878.48 ‡i .K46 2015"));
		bibRec.holdings.add(holdRec);
		holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "9193817";
		holdRec.modifiedDate = "20150701135403.0";
		holdRec.leader = "00182cx  a22000851  4500";
		holdRec.controlFields.add(new ControlField(1,"001","9193817"));
		holdRec.controlFields.add(new ControlField(2,"004","8855036"));
		holdRec.controlFields.add(new ControlField(3,"005","20150701135403.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1503120u||||8|||4001uu|||0000000"));
		holdRec.dataFields.add(new DataField(5,"852",'8',' ',"‡b mann ‡h TD878.48 ‡i .K46 2015"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10155075|20170306131913\n"+
		"holdings_display: 10155076|20170306132457\n"+
		"holdings_display: 9193817|20150701135403\n"+
		"lc_callnum_full: TD878.48 .K46 2015\n"+
		"lc_callnum_full: TD878.48 .K46 2015\n"+
		"lc_callnum_full: TD878.48 .K46 2015\n"+
		"callnum_sort: TD878.48 .K46 2015\n"+
		"item_display: 9758041|9193817\n"+
		"item_display: 10159960|10155075\n"+
		"item_display: 10159965|10155076\n"+
		"location_facet: Mann Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testMultipleVolumes()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "03815cam a2200469 i 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 2 volumes : ‡b illustrations ; ‡c 24 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10086111";
		holdRec.modifiedDate = "20170126080455.0";
		holdRec.leader = "00211cv  a22000974  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10086111"));
		holdRec.controlFields.add(new ControlField(2,"004","9763619"));
		holdRec.controlFields.add(new ControlField(3,"005","20170126080455.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1701192p    8   1001uu   0000000"));
		holdRec.dataFields.add(new DataField(5,"852",'0','1',"‡b mann,cd ‡h ZA4080.4 ‡i .C87 2016"));
		holdRec.dataFields.add(new DataField(6,"866",'4','1',"‡8 0 ‡h v.1-2"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10086111|20170126080455\n"+
		"lc_callnum_full: ZA4080.4 .C87 2016\n"+
		"callnum_sort: ZA4080.4 .C87 2016\n"+
		"lc_callnum_facet: Z - Bibliography, Library Science, Information Resources\n"+
		"lc_callnum_facet: Z - Bibliography, Library Science, Information Resources:ZA - Information Resources\n"+
		"lc_callnum_facet: Z - Bibliography, Library Science, Information Resources:ZA - Information Resources:"
		+ "ZA4050-4775 - Information in specific formats or media\n"+
		"lc_callnum_facet: Z - Bibliography, Library Science, Information Resources:ZA - Information Resources:"
		+ "ZA4050-4775 - Information in specific formats or media:ZA4050-4480 - Electronic information resources\n"+
		"item_display: 10132536|10086111|20170126080507\n"+
		"item_display: 10132539|10086111|20170126080516\n"+
		"location_facet: Mann Library\n"+
		"online: At the Library\n"+
		"multivol_b: true\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testMainItemMultivol()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "02631ccm a2200541Ii 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 1 score (182 pages) : ‡b facsimiles, portraits ;"
				+ " ‡c 38 cm + ‡e 1 critical report (59 pages) ; ‡c 29 cm."));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10078053";
		holdRec.modifiedDate = "20170103162310.0";
		holdRec.leader = "00211cv  a22000974  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10078053"));
		holdRec.controlFields.add(new ControlField(2,"004","9755089"));
		holdRec.controlFields.add(new ControlField(3,"005","20170103162310.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1701032u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(5,"852",'0',' ',"‡b mus,ref ‡h M3 ‡i .W42 1996 ‡m + ser.1 v.3"));
		holdRec.dataFields.add(new DataField(6,"866",'4','1',"‡8 0 ‡a score; critical report"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10078053|20170103162310\n"+
		"lc_callnum_full: M3 .W42 1996\n"+
		"callnum_sort: M3 .W42 1996\n"+
		"lc_callnum_facet: M - Music\n"+
		"lc_callnum_facet: M - Music:M - Music\n"+
		"lc_callnum_facet: M - Music:M - Music:M3-3.3 - Collected works of individual composers\n"+
		"item_display: 10117928|10078053\n"+
		"item_display: 10117929|10078053|20170103164717\n"+
		"location_facet: Music Library\n"+
		"online: At the Library\n"+
		"mainitem_b: true\n"+
		"multivolwblank_b: true\n"+
		"multivol_b: true\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	//TODO Remove or fix this test. It fails because there's now an item record on the holding //@Test
	public void testBoundWith()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "02445cam a2200517Ii 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 384 pages : ‡b illustrations ; ‡c 25 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10016824";
		holdRec.modifiedDate = "20161021133727.0";
		holdRec.leader = "00269cx  a22000971  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10016824"));
		holdRec.controlFields.add(new ControlField(2,"004","9690597"));
		holdRec.controlFields.add(new ControlField(3,"005","20161021133727.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1610212u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(5,"852",'0','0',"‡b mann ‡h SF1 ‡i .E89 no.137"
				+ " ‡z Also catalogued as part of the serial: EAAP publication."));
		holdRec.dataFields.add(new DataField(6,"876",' ',' ',"‡p 31924123150835"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10016824|20161021133727\n"+
		"lc_callnum_full: SF1 .E89 no.137\n"+
		"callnum_sort: SF1 .E89 no.137\n"+
		"lc_callnum_facet: S - Agriculture\n"+
		"lc_callnum_facet: S - Agriculture:SF - Animal Culture\n"+
		"bound_with_json: {\"item_enum\":\"\",\"item_id\":10035199,\"mfhd_id\":\"10016824\","
		+ "\"barcode\":\"31924123150835\"}\n"+
		"location_facet: Mann Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n"+
		"suppress_bound_with_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testSuppressBoundWith()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "00785cam a2200217 a 4500";
		bibRec.dataFields.add(new DataField(1,"300",' ',' ',"‡a 186 p. : ‡b ill., front., plates."));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "3639439";
		holdRec.modifiedDate = "20170523083204.0";
		holdRec.leader = "00352cx  a2200121z  4500";
		holdRec.controlFields.add(new ControlField(1,"001","3639439"));
		holdRec.controlFields.add(new ControlField(2,"004","3099378"));
		holdRec.controlFields.add(new ControlField(3,"005","20170523083204.0"));
		holdRec.controlFields.add(new ControlField(4,"008","0005182u    8   4001uu   0000000"));
		holdRec.dataFields.add(new DataField(5,"014",'1',' ',"‡a AQC4384CU001"));
		holdRec.dataFields.add(new DataField(6,"014",'0',' ',"‡9 003637022"));
		holdRec.dataFields.add(new DataField(7,"852",'0','0',"‡b olin,anx ‡h Film 2600 1774-1850 Reel A-1, no.10."
				+ " ‡z Also cataloged as part of: Wright American fiction."));
		holdRec.dataFields.add(new DataField(8,"876",' ',' ',"‡p 31924101012320 ‡x analytic"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 3639439|20170523083204\n"+
		"lc_callnum_full: Film 2600 1774-1850 Reel A-1, no.10.\n"+
		"callnum_sort: Film 2600 1774-1850 Reel A-1, no.10.\n"+
		"item_display: 10276119|3639439\n"+
		"location_facet: Library Annex\n"+
		"online: At the Library\n"+
		"multivol_b: false\n"+
		"suppress_bound_with_b: true\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

//	@Test
	public void testNewAndNoteworthyCallNumberManipulationNonFiction()
			throws ClassNotFoundException, SQLException, IOException {

		// Non fiction 9753057
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "02677cam a2200409 i 4500";
		bibRec.controlFields.add(new ControlField(1,"008","160614s2016    caua          000 0 eng"));
		bibRec.dataFields.add(new DataField(2,"100",'1',' ',"‡a Neely, Nick, ‡e author."));
		bibRec.dataFields.add(new DataField(3,"300",' ',' ',"‡a 215 pages : ‡b illustrations ; ‡c 24 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10076515";
		holdRec.modifiedDate = "20170119150507.0";
		holdRec.leader = "00352cx  a2200121z  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10076515"));
		holdRec.controlFields.add(new ControlField(2,"004","9753057"));
		holdRec.controlFields.add(new ControlField(3,"005","20170119150507.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1612200u||||8|||4001uu|||0000000"));
		holdRec.dataFields.add(new DataField(7,"852",'0','0',"‡b olin ‡k New & Noteworthy Books ‡h AC8 ‡i .N35 2016"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10076515|20170119150507\n"+
		"lc_callnum_full: AC8 .N35 2016\n"+
		"lc_callnum_full: New & Noteworthy Books AC8 .N35 2016\n"+
		"callnum_sort: AC8 .N35 2016\n"+
		"lc_callnum_facet: A - General\n"+
		"lc_callnum_facet: A - General:AC - Collections, Series, Collected works\n"+
		"lc_callnum_facet: A - General:AC - Collections, Series, Collected works:AC1-195 - Collections of monographs, essays, etc.\n"+
		"lc_callnum_facet: A - General:AC - Collections, Series, Collected works:AC1-195 - Collections of monographs, essays, etc.:AC1-8 - American and English\n"+
		"item_display: 10125124|10076515|20170119150458\n"+
		"location_facet: Olin Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testNewAndNoteworthyCallNumberManipulationFiction()
			throws ClassNotFoundException, SQLException, IOException {

		// Fiction 8573311
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "02421cam a2200505 i 4500";
		bibRec.controlFields.add(new ControlField(1,"008","140225s2014    nyuaf    b    000 1beng"));
		bibRec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Bird, Kai."));
		bibRec.dataFields.add(new DataField(2,"300",' ',' ',
				"‡a xiv, 430 pages, 16 pages of unnumbered plates : ‡b illustrations ; ‡c 25 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "9094799";
		holdRec.modifiedDate = "20141215141205.0";
		holdRec.leader = "00352cx  a2200121z  4500";
		holdRec.controlFields.add(new ControlField(1,"001","9094799"));
		holdRec.controlFields.add(new ControlField(2,"004","8753311"));
		holdRec.controlFields.add(new ControlField(3,"005","20141215141205.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1612200u||||8|||4001uu|||0000000"));
		holdRec.dataFields.add(new DataField(7,"852",'0','0',
				"‡b olin ‡k New & Noteworthy Books ‡h HN49.V64 ‡i L37 2014"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 9094799|20141215141205\n"+
		"lc_callnum_full: HN49.V64 L37 2014\n"+
		"lc_callnum_full: New & Noteworthy Books HN49.V64 L37 2014\n"+
		"callnum_sort: HN49.V64 L37 2014\n"+
		"lc_callnum_facet: H - Social Sciences\n"+
		"lc_callnum_facet: H - Social Sciences:HN - Social History & Conditions, Problems & Reform\n"+
		"item_display: 9610051|9094799\n"+
		"location_facet: Olin Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testNewAndNoteworthyCallNumberManipulationOversize()
			throws ClassNotFoundException, SQLException, IOException {

		// Oversize 9928811
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "03325cam a2200589 i 4500";
		bibRec.controlFields.add(new ControlField(1,"008","160401s2016    xnaa   e b    001 0deng d"));
		bibRec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Coulthart, Ross, ‡e author."));
		bibRec.dataFields.add(new DataField(2,"300",' ',' ',"‡a 397 pages : ‡b illustrations ; ‡c 27 cm."));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10245520";
		holdRec.modifiedDate = "20170610162240.0";
		holdRec.leader = "00352cx  a2200121z  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10245520"));
		holdRec.controlFields.add(new ControlField(2,"004","9928811"));
		holdRec.controlFields.add(new ControlField(3,"005","20170610162240.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1706082u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(7,"852",'0','0',"‡b olin ‡k New & Noteworthy Books ‡h D547.A8 ‡i C68 2016 ‡m +"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10245520|20170610162240\n"+
		"lc_callnum_full: D547.A8 C68 2016\n"+
		"lc_callnum_full: New & Noteworthy Books D547.A8 C68 2016\n"+
		"callnum_sort: D547.A8 C68 2016\n"+
		"lc_callnum_facet: D - World History\n"+
		"lc_callnum_facet: D - World History:D - History (General)\n"+
		"lc_callnum_facet: D - World History:D - History (General):D501-680 - World War I (1914-1918)\n"+
		"item_display: 10309656|10245520|20180702111338\n"+
		"location_facet: Olin Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testLawCollectionFieldOnBook() throws ClassNotFoundException, SQLException, IOException{
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ); // 10161043
		bibRec.leader = "04124cam a2200529 i 4500";
		bibRec.controlFields.add(new ControlField(1,"008","170817t20182018ilua     b    001 0 eng"));
		bibRec.dataFields.add(new DataField(2,"300",' ',' ',"‡a xxi, 479 pages : ‡b illustrations ; ‡c 23 cm"));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10471410";
		holdRec.modifiedDate = "20171128152204.0";
		holdRec.leader = "00352cx  a2200121z  4500";
		holdRec.controlFields.add(new ControlField(1,"001","10471410"));
		holdRec.controlFields.add(new ControlField(2,"004","10161043"));
		holdRec.controlFields.add(new ControlField(3,"005","20171128152204.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1711212u    8   4001uueng0901128"));
		holdRec.dataFields.add(new DataField(7,"852",'0','0',"‡b law ‡h KF318 ‡i .A75 2018"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10471410|20171128152204\n"+
		"lc_callnum_full: KF318 .A75 2018\n"+
		"callnum_sort: KF318 .A75 2018\n"+
		"lc_callnum_facet: K - Law\n"+
		"lc_callnum_facet: K - Law:KF-KFZ - The United States\n"+
		"lc_callnum_facet: K - Law:KF-KFZ - The United States:KF - Federal law.  Common and collective state law\n"+
		"item_display: 10398999|10471410|20200727103334\n"+
		"location_facet: Law Library\n"+
		"online: At the Library\n"+
		"collection: Law Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void testLawCollectionFieldOnDigital() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC ); //10115740
		bibRec.leader = "04266cam a22007095i 4500";
		bibRec.controlFields.add(new ControlField(1,"008","170814s2018    gw |    o    |||| 0|eng d"));
		bibRec.dataFields.add(new DataField(1,"050",' ','4',"‡a K3538-3544"));
		bibRec.dataFields.add(new DataField(2,"100",'1',' ',"‡a Zhong, Zhang-Dui. ‡e author."));
		bibRec.dataFields.add(new DataField(3,"300",' ',' ',"‡a 1 online resource."));
		MarcRecord holdRec = new MarcRecord( MarcRecord.RecordType.HOLDINGS );
		holdRec.id = "10426151";
		holdRec.modifiedDate = "20171120151514.0";
		holdRec.leader = "1711200u||||8|||4001uu|||0000000";
		holdRec.controlFields.add(new ControlField(1,"001","10426151"));
		holdRec.controlFields.add(new ControlField(2,"004","10115740"));
		holdRec.controlFields.add(new ControlField(3,"005","20171120151514.0"));
		holdRec.controlFields.add(new ControlField(4,"008","1706082u    8   4001uu   0901128"));
		holdRec.dataFields.add(new DataField(7,"852",'8',' ',"‡b serv,remo ‡h No call number"));
		bibRec.holdings.add(holdRec);
		String expected =
		"holdings_display: 10426151|20171120151514\n"+
		"lc_callnum_full: K3538-3544\n"+
		"lc_bib_display: K3538-3544\n"+
		"callnum_sort: K3538-3544\n"+
		"lc_callnum_facet: K - Law\n"+
		"lc_callnum_facet: K - Law:K - Law in general, Comparative and uniform law, Jurisprudence\n"+
		"lc_callnum_facet: K - Law:K - Law in general, Comparative and uniform law, Jurisprudence:"
		+ "K520-5582 - Comparative law.  International uniform law\n"+
		"lc_callnum_facet: K - Law:K - Law in general, Comparative and uniform law, Jurisprudence:"
		+ "K520-5582 - Comparative law.  International uniform law:"
		+ "K3476-3560 - Public property.  Public restraint on private property\n"+
		"collection: Law Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}

	@Test
	public void callNumberJustLetters() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord bibRec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		bibRec.leader = "01072cam a22002895i 4500";
		bibRec.id = "10858685";
		bibRec.dataFields.add(new DataField(1,"050",' ','4',"‡a PN"));
		String expected =
		"lc_callnum_full: PN\n"+
		"lc_bib_display: PN\n"+
		"callnum_sort: PN\n" + 
		"lc_callnum_facet: P - Language & Literature\n" + 
		"lc_callnum_facet: P - Language & Literature:PN - Literature (General)\n" + 
		"multivol_b: false\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, config).toString() );
	}
}
