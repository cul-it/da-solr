package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class HoldingsAndItemsTest {

	static SolrBuildConfig config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
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
		"holdings_record_display:"
		+ " {\"id\":\"10121881\",\"modified_date\":\"20170214132422\",\"copy_number\":null,\"callnos\":null,"
		+ "\"notes\":[],\"holdings_desc\":[],\"recent_holdings_desc\":[],\"supplemental_holdings_desc\":[],"
		+ "\"index_holdings_desc\":[],\"locations\":[{\"code\":\"serv,remo\",\"number\":128,"
		+ "\"name\":\"*Networked Resource\",\"library\":null}]}\n"+
		"multivol_b: false\n";
		assertEquals(expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
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
		"holdings_record_display: {\"id\":\"10091152\",\"modified_date\":\"20170321083807\",\"copy_number\":null,"
		+ "\"callnos\":[\"DS665 .C36 2016\"],\"notes\":[],\"holdings_desc\":[],\"recent_holdings_desc\":[],"
		+ "\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],\"locations\":[{\"code\":\"ech\","
		+ "\"number\":13,\"name\":\"Kroch Library Asia\",\"library\":\"Kroch Library Asia\"}]}\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"book\",\"item_id\":\"10165353\",\"year\":"
		+ "\"\",\"item_type_id\":\"3\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\",\"temp_location\":\"0\","
		+ "\"on_reserve\":\"N\",\"item_enum\":\"\",\"item_sequence_number\":\"1\",\"temp_item_type_id\":\"0\","
		+ "\"mfhd_id\":\"10091152\",\"recalls_placed\":\"0\",\"create_date\":\"2017-03-21 08:38:26.0\","
		+ "\"item_barcode\":\"31924124165360\",\"modify_date\":\"\",\"perm_location\":{\"code\":\"ech\","
		+ "\"number\":13,\"name\":\"Kroch Library Asia\",\"library\":\"Kroch Library Asia\"}}\n"+
		"item_display: 10165353|10091152\n"+
		"location_facet: Kroch Library Asia\n"+
		"location: Kroch Library Asia\n"+
		"location: Kroch Library Asia > Kroch Library Asia\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
	}
	
	@Test
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
		"holdings_record_display: {\"id\":\"10155075\",\"modified_date\":\"20170306131913\",\"copy_number\":\"2\","
		+ "\"callnos\":[\"TD878.48 .K46 2015\"],\"notes\":[],\"holdings_desc\":[],\"recent_holdings_desc\":[],"
		+ "\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],\"locations\":[{\"code\":\"mann\","
		+ "\"number\":69,\"name\":\"Mann Library\",\"library\":\"Mann Library\"}]}\n"+
		"holdings_display: 10155076|20170306132457\n"+
		"holdings_record_display: {\"id\":\"10155076\",\"modified_date\":\"20170306132457\",\"copy_number\":\"3\","
		+ "\"callnos\":[\"TD878.48 .K46 2015\"],\"notes\":[],\"holdings_desc\":[],\"recent_holdings_desc\":[],"
		+ "\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],\"locations\":[{\"code\":\"mann\","
		+ "\"number\":69,\"name\":\"Mann Library\",\"library\":\"Mann Library\"}]}\n"+
		"holdings_display: 9193817|20150701135403\n"+
		"holdings_record_display: {\"id\":\"9193817\",\"modified_date\":\"20150701135403\",\"copy_number\":null,"
		+ "\"callnos\":[\"TD878.48 .K46 2015\"],\"notes\":[],\"holdings_desc\":[],\"recent_holdings_desc\":[],"
		+ "\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],\"locations\":[{\"code\":\"mann\","
		+ "\"number\":69,\"name\":\"Mann Library\",\"library\":\"Mann Library\"}]}\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"book\",\"item_id\":\"9758041\","
		+ "\"year\":\"\",\"item_type_id\":\"3\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"\",\"item_sequence_number\":\"1\","
		+ "\"temp_item_type_id\":\"0\",\"mfhd_id\":\"9193817\",\"recalls_placed\":\"0\","
		+ "\"create_date\":\"2015-07-01 13:54:40.0\",\"item_barcode\":\"31924121536126\",\"modify_date\":\"\","
		+ "\"perm_location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","
		+ "\"library\":\"Mann Library\"}}\n"+
		"item_display: 9758041|9193817\n"+
		"item_record_display: {\"copy_number\":\"2\",\"item_type_name\":\"book\",\"item_id\":\"10159960\","
		+ "\"year\":\"\",\"item_type_id\":\"3\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"\",\"item_sequence_number\":\"1\","
		+ "\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10155075\",\"recalls_placed\":\"0\","
		+ "\"create_date\":\"2017-03-06 13:19:45.0\",\"item_barcode\":\"31924123751350\",\"modify_date\":\"\","
		+ "\"perm_location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","
		+ "\"library\":\"Mann Library\"}}\n"+
		"item_display: 10159960|10155075\n"+
		"item_record_display: {\"copy_number\":\"3\",\"item_type_name\":\"book\",\"item_id\":\"10159965\","
		+ "\"year\":\"\",\"item_type_id\":\"3\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"\",\"item_sequence_number\":\"1\","
		+ "\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10155076\",\"recalls_placed\":\"0\","
		+ "\"create_date\":\"2017-03-06 13:25:23.0\",\"item_barcode\":\"31924123751368\",\"modify_date\":\"\","
		+ "\"perm_location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","
		+ "\"library\":\"Mann Library\"}}\n"+
		"item_display: 10159965|10155076\n"+
		"location_facet: Mann Library\n"+
		"location: Mann Library\n"+
		"location: Mann Library > Mann Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n";
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
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
		"holdings_record_display: {\"id\":\"10086111\",\"modified_date\":\"20170126080455\",\"copy_number\":null,"
		+ "\"callnos\":[\"ZA4080.4 .C87 2016\"],\"notes\":[],\"holdings_desc\":[\"\"],\"recent_holdings_desc\":[],"
		+ "\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],\"locations\":[{\"code\":\"mann,cd\","
		+ "\"number\":74,\"name\":\"Mann Library Collection Development (Non-Circulating)\","
		+ "\"library\":\"Mann Library\"}]}\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"nocirc\",\"item_id\":\"10132536\","
		+ "\"year\":\"\",\"item_type_id\":\"9\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"v.1\",\"item_sequence_number\":\"1\","
		+ "\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10086111\",\"recalls_placed\":\"0\","
		+ "\"create_date\":\"2017-01-25 16:34:07.0\",\"item_barcode\":\"31924123105896\","
		+ "\"modify_date\":\"2017-01-26 08:05:07.0\",\"perm_location\":{\"code\":\"mann,cd\",\"number\":74,"
		+ "\"name\":\"Mann Library Collection Development (Non-Circulating)\",\"library\":\"Mann Library\"}}\n"+
		"item_display: 10132536|10086111|20170126080507\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"nocirc\",\"item_id\":\"10132539\","
		+ "\"year\":\"\",\"item_type_id\":\"9\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"v.2\",\"item_sequence_number\":\"2\","
		+ "\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10086111\",\"recalls_placed\":\"0\","
		+ "\"create_date\":\"2017-01-25 16:36:14.0\",\"item_barcode\":\"31924122947132\","
		+ "\"modify_date\":\"2017-01-26 08:05:16.0\",\"perm_location\":{\"code\":\"mann,cd\",\"number\":74,"
		+ "\"name\":\"Mann Library Collection Development (Non-Circulating)\",\"library\":\"Mann Library\"}}\n"+
		"item_display: 10132539|10086111|20170126080516\n"+
		"location_facet: Mann Library\n"+
		"location: Mann Library\n"+
		"location: Mann Library > Mann Library Collection Development (Non-Circulating)\n"+
		"online: At the Library\n"+
		"multivol_b: true\n";
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
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
		"holdings_record_display: {\"id\":\"10078053\",\"modified_date\":\"20170103162310\",\"copy_number\":null,"
		+ "\"callnos\":[\"M3 .W42 1996 + ser.1 v.3\"],\"notes\":[],\"holdings_desc\":[\"score; critical report\"],"
		+ "\"recent_holdings_desc\":[],\"supplemental_holdings_desc\":[],\"index_holdings_desc\":[],"
		+ "\"locations\":[{\"code\":\"mus,ref\",\"number\":93,"
		+ "\"name\":\"Music Library Reference (Non-Circulating)\",\"library\":\"Music Library\"}]}\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"nocirc\",\"item_id\":\"10117928\","
		+ "\"year\":\"\",\"item_type_id\":\"9\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"score; critical report\","
		+ "\"item_sequence_number\":\"1\",\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10078053\","
		+ "\"recalls_placed\":\"0\",\"create_date\":\"2017-01-03 16:24:01.0\",\"item_barcode\":\"31924123223772\","
		+ "\"modify_date\":\"\",\"perm_location\":{\"code\":\"mus,ref\",\"number\":93,"
		+ "\"name\":\"Music Library Reference (Non-Circulating)\",\"library\":\"Music Library\"}}\n"+
		"item_display: 10117928|10078053\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"nocirc\",\"item_id\":\"10117929\","
		+ "\"year\":\"\",\"item_type_id\":\"9\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"critical report\","
		+ "\"item_sequence_number\":\"2\",\"temp_item_type_id\":\"0\",\"mfhd_id\":\"10078053\","
		+ "\"recalls_placed\":\"0\",\"create_date\":\"2017-01-03 16:24:21.0\",\"item_barcode\":\"31924123223764\","
		+ "\"modify_date\":\"2017-01-03 16:47:17.0\",\"perm_location\":{\"code\":\"mus,ref\",\"number\":93,"
		+ "\"name\":\"Music Library Reference (Non-Circulating)\",\"library\":\"Music Library\"}}\n"+
		"item_display: 10117929|10078053|20170103164717\n"+
		"location_facet: Music Library\n"+
		"location: Music Library\n"+
		"location: Music Library > Music Library Reference (Non-Circulating)\n"+
		"online: At the Library\n"+
		"mainitem_b: true\n"+
		"multivolwblank_b: true\n"+
		"multivol_b: true\n";
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
	}

	@Test
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
		"holdings_record_display: {\"id\":\"10016824\",\"modified_date\":\"20161021133727\",\"copy_number\":null,"
		+ "\"callnos\":[\"SF1 .E89 no.137\"],\"notes\":[\"Also catalogued as part of the serial: EAAP"
		+ " publication.\"],\"holdings_desc\":[],\"recent_holdings_desc\":[],\"supplemental_holdings_desc\":[],"
		+ "\"index_holdings_desc\":[],\"locations\":[{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","
		+ "\"library\":\"Mann Library\"}]}\n"+
		"barcode_addl_t: 31924123150835\n"+
		"bound_with_json: {\"item_enum\":\"\",\"item_id\":10035199,\"mfhd_id\":\"10016824\","
		+ "\"barcode\":\"31924123150835\"}\n"+
		"location_facet: Mann Library\n"+
		"location: Mann Library\n"+
		"location: Mann Library > Mann Library\n"+
		"online: At the Library\n"+
		"multivol_b: false\n"+
		"suppress_bound_with_b: false\n";
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
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
		"holdings_record_display: {\"id\":\"3639439\",\"modified_date\":\"20170523083204\",\"copy_number\":null,"
		+ "\"callnos\":[\"Film 2600 1774-1850 Reel A-1, no.10.\"],\"notes\":[\"Also cataloged as part of: Wright"
		+ " American fiction.\"],\"holdings_desc\":[],\"recent_holdings_desc\":[],\"supplemental_holdings_desc\":[],"
		+ "\"index_holdings_desc\":[],\"locations\":[{\"code\":\"olin,anx\",\"number\":101,\"name\":"
		+ "\"Library Annex\",\"library\":\"Library Annex\"}]}\n"+
		"item_record_display: {\"copy_number\":\"1\",\"item_type_name\":\"microform\",\"item_id\":\"10276119\","
		+ "\"year\":\"\",\"item_type_id\":\"19\",\"chron\":\"\",\"caption\":\"\",\"holds_placed\":\"0\","
		+ "\"temp_location\":\"0\",\"on_reserve\":\"N\",\"item_enum\":\"Filmed with:\",\"item_sequence_number\":"
		+ "\"1\",\"temp_item_type_id\":\"0\",\"mfhd_id\":\"3639439\",\"recalls_placed\":\"0\",\"create_date\":"
		+ "\"2017-05-23 08:38:13.0\",\"item_barcode\":\"\",\"modify_date\":\"\",\"perm_location\":{\"code\":"
		+ "\"olin,anx\",\"number\":101,\"name\":\"Library Annex\",\"library\":\"Library Annex\"}}\n"+
		"item_display: 10276119|3639439\n"+
		"barcode_addl_t: 31924101012320\n"+
		"location_facet: Library Annex\n"+
		"location: Library Annex\n"+
		"location: Library Annex > Library Annex\n"+
		"online: At the Library\n"+
		"multivol_b: false\n"+
		"suppress_bound_with_b: true\n";
//		System.out.println(HoldingsAndItems.generateSolrFields(bibRec,config).toString().replaceAll("\"","\\\\\""));
		assertEquals( expected, HoldingsAndItems.generateSolrFields(bibRec, config).toString() );
	}
}
