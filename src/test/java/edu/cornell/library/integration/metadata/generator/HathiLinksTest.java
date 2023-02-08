package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class HathiLinksTest extends DbBaseTest {
	SolrFieldGenerator gen = new HathiLinks();

//	@BeforeClass
//	public static void setup() {
//		config = Config.loadConfig(Config.getRequiredArgsForDB("Hathi"));
//	}

	@BeforeClass
	public static void setup() throws IOException {
		setup("Hathi");
	}

	@Test
	public void testNoHathiLink() throws SQLException, IOException, ClassNotFoundException {
		assertEquals("",this.gen.generateSolrFields(
				new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC),config).toString());
	}

	@Test
	public void testTitleLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "318";
		String expected =
		"notes_t: HathiTrust (multiple volumes)\n"+
		"url_access_json: {\"description\":\"HathiTrust (multiple volumes)\",\"url\":"
		+ "\"http://catalog.hathitrust.org/Record/008595162\"}\n"+
		"online: Online\n"+
		"hathi_title_data: 008595162\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testVolumeLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "178";
		String expected =
		"notes_t: HathiTrust\n"+
		"url_access_json: {\"description\":\"HathiTrust\",\"url\":\"http://hdl.handle.net/2027/coo.31924005214295\"}\n"+
		"online: Online\n"+
		"hathi_title_data: 100174680\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}
/*
	@Test
	public void testRestrictedLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "4";
		String expected =
		"url_other_display: http://catalog.hathitrust.org/Record/009226070"
		+ "|HathiTrust – Access limited to full-text search\n"+
		"notes_t: HathiTrust – Access limited to full-text search\n"+
		"hathi_title_data: 009226070\n"+
		"notes_t: Connect to full text. Access limited to authorized subscribers.\n" + 
		"url_access_json: {\"description\":\"Connect to full text. Access limited to authorized subscribers.\","
		+ "\"url\":\"https://hdl.handle.net/2027/coo.31924003850009?urlappend=%3B"
		+ "signon=swle:https://shibidp.cit.cornell.edu/idp/shibboleth\"}\n" + 
		"online: Online\n" + 
		"notes_t: Information for users about temporary access\n" + 
		"url_access_json: {\"description\":\"Information for users about temporary access\","
		+ "\"url\":\"https://www.hathitrust.org/ETAS-User-Information\"}\n" + 
		"online: Online\n" + 
		"etas_facet: 1\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void blockEtasLinkWhenNobody() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "101888";
		assertEquals( "", this.gen.generateSolrFields(rec, config).toString() );
	}
*/

	@Test
	public void blockPublicDomainLinkWhenPrivate() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "3776236";
		assertEquals( "", this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testMicrosoftLSDILink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "1460864";
		String expected =
		"notes_t: HathiTrust\n"+
		"url_access_json: {\"description\":\"HathiTrust\","
		+ "\"url\":\"http://hdl.handle.net/2027/coo1.ark:/13960/t20c5hb54\"}\n"+
		"online: Online\n"+
		"hathi_title_data: 100763896\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}
/*
	@Test
	public void etaLink() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "1000002";
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)19326335"));
		String expected =
		"notes_t: Connect to full text. Access limited to authorized subscribers.\n" + 
		"url_access_json: {\"description\":\"Connect to full text. Access limited to authorized subscribers.\","
		+ "\"url\":\"https://catalog.hathitrust.org/Record/000630225?"
		+ "signon=swle:https://shibidp.cit.cornell.edu/idp/shibboleth\"}\n" + 
		"online: Online\n" + 
		"notes_t: Information for users about temporary access\n" + 
		"url_access_json: {\"description\":\"Information for users about temporary access\","
		+ "\"url\":\"https://www.hathitrust.org/ETAS-User-Information\"}\n" + 
		"online: Online\n" + 
		"etas_facet: 2\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}
*/
	@Test
	public void emptyOCLC() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.id = "8396569";
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)"));
		assertEquals( "", this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void oddbehavior() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.dataFields.add(new DataField(1,"035",' ',' ',"‡a (OCoLC)61353090"));
		System.out.println(this.gen.generateSolrFields(rec, config).toString());
	}
}
