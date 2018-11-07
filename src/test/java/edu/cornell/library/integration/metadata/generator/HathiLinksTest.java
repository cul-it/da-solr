package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class HathiLinksTest {

	SolrFieldGenerator gen = new HathiLinks();
	static Config config = null;

	@BeforeClass
	public static void setup() {
		config = Config.loadConfig(null,Config.getRequiredArgsForDB("Hathi"));
	}

	@Test
	public void testNoHathiLink() throws SQLException, IOException, ClassNotFoundException {
		assertEquals("",gen.generateSolrFields(
				new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC),config).toString());
	}

	@Test
	public void testTitleLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.dataFields.add( new DataField( 1, "903",' ',' ',"‡p 31924090258827"));
		rec.dataFields.add( new DataField( 2, "903",' ',' ',"‡p 31924090258827"));
		String expected =
		"url_access_display: http://catalog.hathitrust.org/Record/008595162|HathiTrust (multiple volumes)\n"+
		"notes_t: HathiTrust (multiple volumes)\n"+
		"url_access_json: {\"description\":\"HathiTrust (multiple volumes)\",\"url\":"
		+ "\"http://catalog.hathitrust.org/Record/008595162\"}\n"+
		"online: Online\n"+
		"hathi_title_data: 008595162\n";
		assertEquals( expected, gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testVolumeLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.dataFields.add( new DataField( 1, "903",' ',' ',"‡p 31924005214295"));
		String expected =
		"url_access_display: http://hdl.handle.net/2027/coo.31924005214295|HathiTrust\n"+
		"notes_t: HathiTrust\n"+
		"url_access_json: {\"description\":\"HathiTrust\",\"url\":\"http://hdl.handle.net/2027/coo.31924005214295\"}\n"+
		"online: Online\n"+
		"hathi_title_data: 100174680\n";
		assertEquals( expected, gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testRestrictedLink() throws SQLException, IOException, ClassNotFoundException {
		MarcRecord rec = new MarcRecord( MarcRecord.RecordType.BIBLIOGRAPHIC );
		rec.dataFields.add( new DataField( 1, "903",' ',' ',"‡p 31924014757649"));
		rec.dataFields.add( new DataField( 2, "903",' ',' ',"‡p 31924003850009"));
		String expected =
		"url_other_display: http://catalog.hathitrust.org/Record/009226070"
		+ "|HathiTrust – Access limited to full-text search\n"+
		"notes_t: HathiTrust – Access limited to full-text search\n"+
		"hathi_title_data: 009226070\n";
//		System.out.println(HathiLinks.generateSolrFields(rec, config).toString().replaceAll("\"", "\\\\\""));
		assertEquals( expected, gen.generateSolrFields(rec, config).toString() );
	}
}
