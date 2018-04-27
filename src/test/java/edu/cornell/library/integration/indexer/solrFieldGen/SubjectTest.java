package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class SubjectTest {

	static SolrBuildConfig config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
	}

	@Test
	public void testAuthorizedNoFAST() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','0',"‡a Submerged lands ‡z United States."));
		String expected =
		"subject_t: Submerged lands > United States\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_topic_facet: Submerged lands > United States\n"+
		"subject_topic_filing: submerged lands 0000 united states\n"+
		"subject_json: [{\"subject\":\"Submerged lands\",\"authorized\":true,\"type\":\"Topical Term\"},"
		+ "{\"subject\":\"United States.\",\"authorized\":false}]\n"+
		"subject_display: Submerged lands > United States\n"+
		"authority_subject_t: Submerged coastal lands\n"+
		"authority_subject_t: Tidelands\n"+
		"authority_subject_t: Lands under the marginal sea\n"+
		"authority_subject_t: Lands beneath navigable waters\n"+
		"fast_b: false\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testAuthorizedWithFAST() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','0',"‡a Submerged lands ‡z United States."));
		rec.dataFields.add(new DataField(9,"650",' ','7',"‡a Submerged lands ‡2 fast ‡0 (OCoLC)fst01136664"));
		String expected =
		"subject_t: Submerged lands > United States\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_topic_facet: Submerged lands > United States\n"+
		"subject_topic_filing: submerged lands 0000 united states\n"+
		"subject_t: Submerged lands\n"+
		"fast_topic_facet: Submerged lands\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_json: [{\"subject\":\"Submerged lands\",\"authorized\":true,\"type\":\"Topical Term\"},"
		+ "{\"subject\":\"United States.\",\"authorized\":false}]\n"+
		"subject_display: Submerged lands > United States\n"+
		"authority_subject_t: Submerged coastal lands\n"+
		"authority_subject_t: Tidelands\n"+
		"authority_subject_t: Lands under the marginal sea\n"+
		"authority_subject_t: Lands beneath navigable waters\n"+
		"fast_b: true\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testChronFAST() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"648",' ','7',"‡a 2000-2099 ‡2 fast"));
		String expected =
		"subject_t: 2000-2099\n"+
		"fast_era_facet: 2000-2099\n"+
		"subject_era_facet: 2000-2099\n"+
		"subject_era_filing: 2000 2099\n"+
		"subject_json: [{\"subject\":\"2000-2099\",\"authorized\":false,\"type\":\"Chronological Term\"}]\n"+
		"subject_display: 2000-2099\n"+
		"fast_b: true\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testComplex610() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"610",'2','0',"‡a Jesuits. ‡b Congregatio Generalis ‡n (32nd :"
				+ " ‡d 1974-1975 : ‡c Rome, Italy). ‡t Decree Four."));
		String expected =
		"subject_t: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four\n"+
		"subject_json: [{\"subject\":\"Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four.\",\"authorized\":true,\"type\":\"Work\"}]\n"+
		"subject_display: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"authority_subject_t: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Jesuits today\n"+
		"authority_subject_t: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Our mission today\n"+
		"fast_b: false\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());

		rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"610",'2','0',"‡a Bible. ‡k Paraphrases. ‡p O.T. ‡l English."));
		expected =
		"subject_t: Bible. | Paraphrases. O.T. English\n"+
		"subject_work_facet: Bible. | Paraphrases. O.T. English\n"+
		"subject_work_filing: bible 0000 paraphrases ot english\n"+
		"subject_json: [{\"subject\":\"Bible. | Paraphrases. O.T. English.\","
		+ "\"authorized\":false,\"type\":\"Work\"}]\n"+
		"subject_display: Bible. | Paraphrases. O.T. English\n"+
		"fast_b: false\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testNonRoman610() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,4,"610",'2','0',"‡6 880-04 ‡a Asahi Shinbun ‡v Indexes.",false));
		rec.dataFields.add(new DataField(2,4,"610",'2','0',"‡6 610-04/$1 ‡a 朝日新聞 ‡x Indexes.",true));
		String expected =
		"subject_t: 朝日新聞 > Indexes\n"+
		"subject_t: Asahi Shinbun > Indexes\n"+
		"subject_corp_facet: Asahi Shinbun > Indexes\n"+
		"subject_corp_filing: asahi shinbun 0000 indexes\n"+
		"subject_corp_facet: 朝日新聞 > Indexes\n"+
		"subject_corp_filing: 朝日新聞 0000 indexes\n"+
		"subject_corp_facet: 朝日新聞\n"+
		"subject_corp_filing: 朝日新聞\n"+
		"subject_corp_facet: Asahi Shinbun\n"+
		"subject_corp_filing: asahi shinbun\n"+
		"subject_json: [{\"subject\":\"朝日新聞\",\"authorized\":false,\"type\":\"Corporate Name\"},"
		+ "{\"subject\":\"Indexes.\",\"authorized\":false}]\n"+
		"subject_json: [{\"subject\":\"Asahi Shinbun\",\"authorized\":false,\"type\":\"Corporate Name\"},"
		+ "{\"subject\":\"Indexes.\",\"authorized\":false}]\n"+
		"subject_display: 朝日新聞 > Indexes\n"+
		"subject_display: Asahi Shinbun > Indexes\n"+
		"fast_b: false\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}

	@Test
	public void test653() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"653",' ',' ',"‡a Textiles and Fashion Design"));
//		System.out.println(Subject.generateSolrFields(rec, config).toString().replaceAll("\"", "\\\\\""));
		String expected =
		"sixfivethree: Textiles and Fashion Design\n"+
		"subject_t: Textiles and Fashion Design\n"+
		"subject_gen_facet: Textiles and Fashion Design\n"+
		"subject_gen_filing: textiles and fashion design\n"+
		"subject_json: [{\"subject\":\"Textiles and Fashion Design\",\"authorized\":false,\"type\":\"General Heading\"}]\n"+
		"subject_display: Textiles and Fashion Design\n"+
		"fast_b: false\n";
		assertEquals(expected,Subject.generateSolrFields(rec, config).toString());
	}
}
