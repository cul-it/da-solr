package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

@SuppressWarnings("static-method")
public class HathiLinksTest {

	static SolrBuildConfig config = null;
	static Connection conn = null;

	@BeforeClass
	public static void setup() throws ClassNotFoundException, SQLException {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("CallNos");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
		conn = config.getDatabaseConnection("Hathi");
	}

	@Test
	public void testNoHathiLink() throws SQLException, IOException {
		HathiLinks.SolrFieldValueSet vals = HathiLinks.generateSolrFields(conn,new HashSet<String>());
		assertEquals(0, vals.fields.size());
	}

	@Test
	public void testTitleLink() throws SQLException, IOException {
		Collection<String> barcodes = new HashSet<>();
		barcodes.add("31924090258827");
		barcodes.add("31924022516268");
		HathiLinks.SolrFieldValueSet vals = HathiLinks.generateSolrFields(conn,barcodes);
		assertEquals(5, vals.fields.size());
		assertEquals("url_access_display",vals.fields.get(0).fieldName);
		assertEquals("http://catalog.hathitrust.org/Record/008595162|HathiTrust (multiple volumes)",
				vals.fields.get(0).fieldValue);
		assertEquals("notes_t",vals.fields.get(1).fieldName);
		assertEquals("HathiTrust (multiple volumes)",vals.fields.get(1).fieldValue);
		assertEquals("url_access_json",vals.fields.get(2).fieldName);
		assertEquals("{\"description\":\"HathiTrust (multiple volumes)\","
				+ "\"url\":\"http://catalog.hathitrust.org/Record/008595162\"}",
				vals.fields.get(2).fieldValue);
		assertEquals("hathi_title_data",vals.fields.get(3).fieldName);
		assertEquals("008595162",vals.fields.get(3).fieldValue);
		assertEquals("online",vals.fields.get(4).fieldName);
		assertEquals("Online",vals.fields.get(4).fieldValue);
	}

	@Test
	public void testVolumeLink() throws SQLException, IOException {
		Collection<String> barcodes = new HashSet<>();
		barcodes.add("31924005214295");
		HathiLinks.SolrFieldValueSet vals = HathiLinks.generateSolrFields(conn,barcodes);
		assertEquals(5, vals.fields.size());
		assertEquals("url_access_display",vals.fields.get(0).fieldName);
		assertEquals("http://hdl.handle.net/2027/coo.31924005214295|HathiTrust",
				vals.fields.get(0).fieldValue);
		assertEquals("notes_t",vals.fields.get(1).fieldName);
		assertEquals("HathiTrust",vals.fields.get(1).fieldValue);
		assertEquals("url_access_json",vals.fields.get(2).fieldName);
		assertEquals("{\"description\":\"HathiTrust\","
				+ "\"url\":\"http://hdl.handle.net/2027/coo.31924005214295\"}",
				vals.fields.get(2).fieldValue);
		assertEquals("hathi_title_data",vals.fields.get(3).fieldName);
		assertEquals("100174680",vals.fields.get(3).fieldValue);
		assertEquals("online",vals.fields.get(4).fieldName);
		assertEquals("Online",vals.fields.get(4).fieldValue);
	}

	@Test
	public void testRestrictedLink() throws SQLException, IOException {
		Collection<String> barcodes = new HashSet<>();
		barcodes.add("31924014757649");
		barcodes.add("31924003850009");
		HathiLinks.SolrFieldValueSet vals = HathiLinks.generateSolrFields(conn,barcodes);
		assertEquals(3, vals.fields.size());
		assertEquals("url_other_display",vals.fields.get(0).fieldName);
		assertEquals("http://catalog.hathitrust.org/Record/009226070|HathiTrust – Access limited to full-text search",
				vals.fields.get(0).fieldValue);
		assertEquals("notes_t",vals.fields.get(1).fieldName);
		assertEquals("HathiTrust – Access limited to full-text search",vals.fields.get(1).fieldValue);
		assertEquals("hathi_title_data",vals.fields.get(2).fieldName);
		assertEquals("009226070",vals.fields.get(2).fieldValue);
	}

}
