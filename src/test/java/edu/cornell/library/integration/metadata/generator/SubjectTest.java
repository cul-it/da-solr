package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class SubjectTest extends DbBaseTest {
	SolrFieldGenerator gen = new Subject();

//	@BeforeClass
//	public static void setup() {
//		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
//		config = Config.loadConfig(requiredArgs);
//	}

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		setup("Headings");
	}

	@Test
	public void testAuthorizedNoFAST() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','0',"‡a Submerged lands ‡z United States."));
		String expected =
		"subject_t: Submerged lands > United States\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_topic_lc_facet: Submerged lands\n"+
		"subject_topic_lc_filing: submerged lands\n"+
		"subject_topic_facet: Submerged lands > United States\n"+
		"subject_topic_filing: submerged lands 0000 united states\n"+
		"subject_topic_lc_facet: Submerged lands > United States\n"+
		"subject_topic_lc_filing: submerged lands 0000 united states\n"+
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+
		"subject_json: [{\"subject\":\"Submerged lands\",\"authorized\":true,\"type\":\"Topical Term\"},"
		+ "{\"subject\":\"United States.\",\"authorized\":false}]\n"+
		"subject_display: Submerged lands > United States\n"+
		"authority_subject_t: Submerged coastal lands\n"+
		"authority_subject_t: Tidelands\n"+
		"authority_subject_t: Lands under the marginal sea\n"+
		"authority_subject_t: Lands beneath navigable waters\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testAuthorizedWithFAST() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','0',"‡a Submerged lands ‡z United States."));
		rec.dataFields.add(new DataField(9,"650",' ','7',"‡a Submerged lands ‡2 fast ‡0 (OCoLC)fst01136664"));
		String expected =
		"subject_t: Submerged lands > United States\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_topic_lc_facet: Submerged lands\n"+
		"subject_topic_lc_filing: submerged lands\n"+
		"subject_topic_facet: Submerged lands > United States\n"+
		"subject_topic_filing: submerged lands 0000 united states\n"+
		"subject_topic_lc_facet: Submerged lands > United States\n"+
		"subject_topic_lc_filing: submerged lands 0000 united states\n"+
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+
		"subject_t: Submerged lands\n"+
		"fast_topic_facet: Submerged lands\n"+
		"subject_topic_facet: Submerged lands\n"+
		"subject_topic_filing: submerged lands\n"+
		"subject_topic_fast_facet: Submerged lands\n"+
		"subject_topic_fast_filing: submerged lands\n"+
		"subject_json: [{\"subject\":\"Submerged lands\",\"authorized\":true,\"type\":\"Topical Term\"},"
		+ "{\"subject\":\"United States.\",\"authorized\":false}]\n"+
		"subject_display: Submerged lands > United States\n"+
		"authority_subject_t: Submerged coastal lands\n"+
		"authority_subject_t: Tidelands\n"+
		"authority_subject_t: Lands under the marginal sea\n"+
		"authority_subject_t: Lands beneath navigable waters\n"+
		"fast_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testChronFAST() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"648",' ','7',"‡a 2000-2099 ‡2 fast"));
		String expected =
		"subject_t: 2000-2099\n"+
		"fast_era_facet: 2000 - 2099\n"+
		"subject_era_facet: 2000-2099\n"+
		"subject_era_filing: 2000 2099\n"+
		"subject_era_fast_facet: 2000-2099\n"+
		"subject_era_fast_filing: 2000 2099\n"+
		"subject_json: [{\"subject\":\"2000-2099\",\"authorized\":false,\"type\":\"Chronological Term\"}]\n"+
		"subject_display: 2000 - 2099\n"+
		"fast_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testChronFASTWithUnwantedSpaces() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"648",' ','7',"‡a 1900 - 1999 ‡2 fast"));
		String expected =
		"subject_t: 1900 - 1999\n" + 
		"fast_era_facet: 1900 - 1999\n" + 
		"subject_era_facet: 1900 - 1999\n" + 
		"subject_era_filing: 1900 1999\n" +
		"subject_era_fast_facet: 1900 - 1999\n" + 
		"subject_era_fast_filing: 1900 1999\n" +
		"subject_json: [{\"subject\":\"1900 - 1999\",\"authorized\":false,\"type\":\"Chronological Term\"}]\n" + 
		"subject_display: 1900 - 1999\n" + 
		"fast_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testComplex610() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"610",'2','0',"‡a Jesuits. ‡b Congregatio Generalis ‡n (32nd :"
				+ " ‡d 1974-1975 : ‡c Rome, Italy). ‡t Decree Four."));
		String expected =
		"subject_t: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four\n"+
		"subject_work_lc_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_lc_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four\n"+
		"subject_corp_lc_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy)\n"+
		"subject_corp_lc_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy\n"+
		"subject_json: [{\"subject\":\"Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four.\",\"authorized\":true,\"type\":\"Work\"}]\n"+
		"subject_display: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"authority_subject_t: Jesuits. Congregatio Generalis. | Jesuits today\n"+
		"authority_subject_t: Jesuits. Congregatio Generalis. | Our mission today\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());

		rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"610",'2','0',"‡a Bible. ‡k Paraphrases. ‡p O.T. ‡l English."));
		expected =
		"subject_t: Bible. | Paraphrases. O.T. English\n"+
		"subject_work_facet: Bible. | Paraphrases. O.T. English\n"+
		"subject_work_filing: bible 0000 paraphrases ot english\n"+
		"subject_work_lc_facet: Bible. | Paraphrases. O.T. English\n"+
		"subject_work_lc_filing: bible 0000 paraphrases ot english\n"+
		"subject_corp_lc_facet: Bible\n"+
		"subject_corp_lc_filing: bible\n"+
		"subject_json: [{\"subject\":\"Bible. | Paraphrases. O.T. English.\","
		+ "\"authorized\":false,\"type\":\"Work\"}]\n"+
		"subject_display: Bible. | Paraphrases. O.T. English\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testPersonTitle600() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"600",'1','0',"‡a Mill, John Stuart, ‡d 1806-1873. ‡t System of logic."));
		String expected =
		"subject_t: Mill, John Stuart, 1806-1873. | System of logic\n"+
		"subject_work_facet: Mill, John Stuart, 1806-1873. | System of logic\n"+
		"subject_work_filing: mill john stuart 1806 1873 0000 system of logic\n"+
		"subject_work_lc_facet: Mill, John Stuart, 1806-1873. | System of logic\n"+
		"subject_work_lc_filing: mill john stuart 1806 1873 0000 system of logic\n"+
		"subject_pers_lc_facet: Mill, John Stuart, 1806-1873\n"+
		"subject_pers_lc_filing: mill john stuart 1806 1873\n"+
		"subject_json: [{\"subject\":\"Mill, John Stuart, 1806-1873. | System of logic.\",\"authorized\":false,\"type\":\"Work\"}]\n"+
		"subject_display: Mill, John Stuart, 1806-1873. | System of logic\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testNonRoman610() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,4,"610",'2','0',"‡6 880-04 ‡a Asahi Shinbun ‡v Indexes.",false));
		rec.dataFields.add(new DataField(2,4,"610",'2','0',"‡6 610-04/$1 ‡a 朝日新聞 ‡x Indexes.",true));
		String expected =
		"subject_t: 朝日新聞 > Indexes\n"+
		"subject_t: Asahi Shinbun > Indexes\n"+
		"subject_corp_facet: 朝日新聞\n"+
		"subject_corp_filing: 朝日新聞\n"+
		"subject_corp_facet: 朝日新聞 > Indexes\n"+
		"subject_corp_filing: 朝日新聞 0000 indexes\n"+
		"subject_corp_facet: Asahi Shinbun\n"+
		"subject_corp_filing: asahi shinbun\n"+
		"subject_corp_lc_facet: Asahi Shinbun\n"+
		"subject_corp_lc_filing: asahi shinbun\n"+
		"subject_corp_facet: Asahi Shinbun > Indexes\n"+
		"subject_corp_filing: asahi shinbun 0000 indexes\n"+
		"subject_corp_lc_facet: Asahi Shinbun > Indexes\n"+
		"subject_corp_lc_filing: asahi shinbun 0000 indexes\n"+
		"subject_sub_lc_facet: Indexes\n"+
		"subject_sub_lc_filing: indexes\n"+
		"subject_json: [{\"subject\":\"朝日新聞\",\"authorized\":false,\"type\":\"Corporate Name\"},"
		+ "{\"subject\":\"Indexes.\",\"authorized\":false}]\n"+
		"subject_json: [{\"subject\":\"Asahi Shinbun\",\"authorized\":false,\"type\":\"Corporate Name\"},"
		+ "{\"subject\":\"Indexes.\",\"authorized\":false}]\n"+
		"subject_display: 朝日新聞 > Indexes\n"+
		"subject_display: Asahi Shinbun > Indexes\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testUnwantedFacetValue() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','4',"‡a Electronic books."));
		String expected =
		"subject_t: Electronic books\n"+
		"subject_topic_filing: electronic books\n"+
		"subject_topic_unk_facet: Electronic books\n"+
		"subject_topic_unk_filing: electronic books\n"+
		"subject_json: [{\"subject\":\"Electronic books.\",\"authorized\":true,\"type\":\"Topical Term\"}]\n"+
		"subject_display: Electronic books\n"+
		"authority_subject_t: Books in machine-readable form\n"+
		"authority_subject_t: Ebooks\n"+
		"authority_subject_t: E-books\n"+
		"authority_subject_t: Online books\n"+
		"authority_subject_t: Digital books\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void test653() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"653",' ',' ',"‡a Textiles and Fashion Design"));
		String expected =
		"sixfivethree: Textiles and Fashion Design\n"+
		"subject_t: Textiles and Fashion Design\n"+
		"subject_gen_facet: Textiles and Fashion Design\n"+
		"subject_gen_filing: textiles and fashion design\n"+
		"subject_gen_unk_facet: Textiles and Fashion Design\n"+
		"subject_gen_unk_filing: textiles and fashion design\n"+
		"keyword_display: Textiles and Fashion Design\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}


	@Test
	public void test653core() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"653",' ',' ',"‡a Art and Architecture (Core)"));
		String expected =
		"sixfivethree: Art and Architecture (Core)\n" +
		"subject_t: Art and Architecture (Core)\n" +
		"subject_gen_facet: Art and Architecture\n" +
		"subject_gen_filing: art and architecture\n" +
		"subject_gen_unk_facet: Art and Architecture\n" +
		"subject_gen_unk_filing: art and architecture\n" +
		"keyword_display: Art and Architecture\n" +
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test //DISCOVERYACCESS-3760
	public void dontSearchOnParentheticalDisamiguationsInAlternateForms()
			throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "10215428";
		rec.dataFields.add(new DataField(1,"651",' ','7',"‡a Cambodia ‡z Svay Riĕng. ‡2 fast ‡0 (OCoLC)fst01878040"));
		assertFalse(this.gen.generateSolrFields(rec, config).toString().contains("Coalition"));
	}

	@Test
	public void swappedOffensiveSubjectTerms() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "10329599";
		rec.dataFields.add(new DataField(1,"650",' ','0',"‡a Illegal aliens ‡z United States."));
		rec.dataFields.add(new DataField(2,"650",' ','0',"‡a Illegal aliens ‡x Government policy ‡z United States."));
		rec.dataFields.add(new DataField(3,"650",' ','0',"‡a Illegal alien children ‡z United States."));
		rec.dataFields.add(new DataField(4,"650",' ','0',"‡a Illegal alien children ‡x Government policy ‡z United States."));
		rec.dataFields.add(new DataField(5,"650",' ','0',"‡a United States ‡x Emigration and immigration ‡x Government policy."));
		rec.dataFields.add(new DataField(6,"650",' ','0',"‡a Emigration and immigration law ‡z United States."));
		rec.dataFields.add(new DataField(7,"650",' ','7',"‡a POLITICAL SCIENCE ‡x American Government. ‡2 bisacsh"));
		rec.dataFields.add(new DataField(8,"650",' ','7',"‡a Illegal aliens ‡z United States. ‡2 sears"));
		rec.dataFields.add(new DataField(9,"650",' ','4',"‡a Immigration law ‡z United States."));
		rec.dataFields.add(new DataField(10,"650",' ','7',"‡a Illegal aliens. ‡2 fast ‡0 (OCoLC)fst00967153"));
		String expected =
		"subject_overlay_facet: Undocumented immigrants\n"+
		"subject_t: Undocumented immigrants > United States\n" + 
		"subject_topic_facet: Undocumented immigrants\n" + 
		"subject_topic_filing: undocumented immigrants\n" + 
		"subject_topic_lc_facet: Illegal aliens\n" + 
		"subject_topic_lc_filing: illegal aliens\n" + 
		"subject_topic_facet: Undocumented immigrants > United States\n" +
		"subject_topic_filing: undocumented immigrants 0000 united states\n" + 
		"subject_topic_lc_facet: Illegal aliens > United States\n" + 
		"subject_topic_lc_filing: illegal aliens 0000 united states\n" + 
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+

		"subject_overlay_facet: Undocumented immigrants\n"+
		"subject_t: Undocumented immigrants > Government policy > United States\n" + 
		"subject_topic_facet: Undocumented immigrants\n" + 
		"subject_topic_filing: undocumented immigrants\n" +
		"subject_topic_lc_facet: Illegal aliens\n" + 
		"subject_topic_lc_filing: illegal aliens\n" + 
		"subject_topic_facet: Undocumented immigrants > Government policy\n" + 
		"subject_topic_filing: undocumented immigrants 0000 government policy\n" +
		"subject_topic_lc_facet: Illegal aliens > Government policy\n" + 
		"subject_topic_lc_filing: illegal aliens 0000 government policy\n" + 
		"subject_topic_facet: Undocumented immigrants > Government policy > United States\n" + 
		"subject_topic_filing: undocumented immigrants 0000 government policy 0000 united states\n" + 
		"subject_topic_lc_facet: Illegal aliens > Government policy > United States\n" +
		"subject_topic_lc_filing: illegal aliens 0000 government policy 0000 united states\n" +
		"subject_sub_lc_facet: Government policy\n"+
		"subject_sub_lc_filing: government policy\n"+
		"subject_sub_lc_facet: Government policy > United States\n"+
		"subject_sub_lc_filing: government policy 0000 united states\n"+
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+

		"subject_overlay_facet: Undocumented immigrant children\n"+
		"subject_t: Undocumented immigrant children > United States\n" + 
		"subject_topic_facet: Undocumented immigrant children\n" + 
		"subject_topic_filing: undocumented immigrant children\n" + 
		"subject_topic_lc_facet: Illegal alien children\n" + 
		"subject_topic_lc_filing: illegal alien children\n" + 
		"subject_topic_facet: Undocumented immigrant children > United States\n" + 
		"subject_topic_filing: undocumented immigrant children 0000 united states\n" +
		"subject_topic_lc_facet: Illegal alien children > United States\n" +
		"subject_topic_lc_filing: illegal alien children 0000 united states\n" +
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+

		"subject_overlay_facet: Undocumented immigrant children\n"+
		"subject_t: Undocumented immigrant children > Government policy > United States\n" + 
		"subject_topic_facet: Undocumented immigrant children\n" + 
		"subject_topic_filing: undocumented immigrant children\n" + 
		"subject_topic_lc_facet: Illegal alien children\n" + 
		"subject_topic_lc_filing: illegal alien children\n" + 
		"subject_topic_facet: Undocumented immigrant children > Government policy\n" + 
		"subject_topic_filing: undocumented immigrant children 0000 government policy\n" + 
		"subject_topic_lc_facet: Illegal alien children > Government policy\n" +
		"subject_topic_lc_filing: illegal alien children 0000 government policy\n" +
		"subject_topic_facet: Undocumented immigrant children > Government policy > United States\n" + 
		"subject_topic_filing: undocumented immigrant children 0000 government policy 0000 united states\n" + 
		"subject_topic_lc_facet: Illegal alien children > Government policy > United States\n" + 
		"subject_topic_lc_filing: illegal alien children 0000 government policy 0000 united states\n" + 
		"subject_sub_lc_facet: Government policy\n"+
		"subject_sub_lc_filing: government policy\n"+
		"subject_sub_lc_facet: Government policy > United States\n"+
		"subject_sub_lc_filing: government policy 0000 united states\n"+
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+

		"subject_t: United States > Emigration and immigration > Government policy\n" + 
		"subject_topic_facet: United States\n" +
		"subject_topic_filing: united states\n" +
		"subject_topic_lc_facet: United States\n" +
		"subject_topic_lc_filing: united states\n" +
		"subject_topic_facet: United States > Emigration and immigration\n" + 
		"subject_topic_filing: united states 0000 emigration and immigration\n" + 
		"subject_topic_lc_facet: United States > Emigration and immigration\n" + 
		"subject_topic_lc_filing: united states 0000 emigration and immigration\n" + 
		"subject_topic_facet: United States > Emigration and immigration > Government policy\n" + 
		"subject_topic_filing: united states 0000 emigration and immigration 0000 government policy\n" + 
		"subject_topic_lc_facet: United States > Emigration and immigration > Government policy\n" + 
		"subject_topic_lc_filing: united states 0000 emigration and immigration 0000 government policy\n" + 
		"subject_sub_lc_facet: Emigration and immigration\n"+
		"subject_sub_lc_filing: emigration and immigration\n"+
		"subject_sub_lc_facet: Emigration and immigration > Government policy\n"+
		"subject_sub_lc_filing: emigration and immigration 0000 government policy\n"+
		"subject_sub_lc_facet: Government policy\n"+
		"subject_sub_lc_filing: government policy\n"+

		"subject_t: Emigration and immigration law > United States\n" + 
		"subject_topic_facet: Emigration and immigration law\n" +
		"subject_topic_filing: emigration and immigration law\n" +
		"subject_topic_lc_facet: Emigration and immigration law\n" +
		"subject_topic_lc_filing: emigration and immigration law\n"+ 
		"subject_topic_facet: Emigration and immigration law > United States\n" + 
		"subject_topic_filing: emigration and immigration law 0000 united states\n" + 
		"subject_topic_lc_facet: Emigration and immigration law > United States\n" + 
		"subject_topic_lc_filing: emigration and immigration law 0000 united states\n" + 
		"subject_sub_lc_facet: United States\n"+
		"subject_sub_lc_filing: united states\n"+

		// related to "bisacsh" vocab term
		"subject_t: POLITICAL SCIENCE > American Government\n"+ 
		"subject_topic_other_facet: POLITICAL SCIENCE\n" +
		"subject_topic_other_filing: political science\n"+ 
		"subject_topic_other_facet: POLITICAL SCIENCE > American Government\n"+ 
		"subject_topic_other_filing: political science 0000 american government\n"+ 
		"subject_sub_other_facet: American Government\n"+
		"subject_sub_other_filing: american government\n"+

		// related to "sears" vocab term
		"subject_overlay_facet: Undocumented immigrants\n"+
		"subject_t: Undocumented immigrants > United States\n" +
		"subject_topic_other_facet: Illegal aliens\n" + 
		"subject_topic_other_filing: illegal aliens\n" + 
		"subject_topic_other_facet: Illegal aliens > United States\n" + 
		"subject_topic_other_filing: illegal aliens 0000 united states\n" + 
		"subject_sub_other_facet: United States\n"+
		"subject_sub_other_filing: united states\n"+

		"subject_t: Immigration law > United States\n" + 
		"subject_topic_facet: Immigration law\n" + 
		"subject_topic_filing: immigration law\n" +
		"subject_topic_unk_facet: Immigration law\n" + 
		"subject_topic_unk_filing: immigration law\n" + 
		"subject_topic_facet: Immigration law > United States\n" + 
		"subject_topic_filing: immigration law 0000 united states\n" + 
		"subject_topic_unk_facet: Immigration law > United States\n" + 
		"subject_topic_unk_filing: immigration law 0000 united states\n" + 
		"subject_sub_unk_facet: United States\n"+
		"subject_sub_unk_filing: united states\n"+

		"subject_overlay_facet: Undocumented immigrants\n"+
		"subject_t: Undocumented immigrants\n" + 
		"fast_topic_facet: Undocumented immigrants\n" +
		"subject_topic_facet: Undocumented immigrants\n" + 
		"subject_topic_filing: undocumented immigrants\n" + 
		"subject_topic_fast_facet: Illegal aliens\n" + 
		"subject_topic_fast_filing: illegal aliens\n" + 

		"subject_json: [{\"subject\":\"Undocumented immigrants\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Undocumented immigrants\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"Government policy\",\"authorized\":false},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Undocumented immigrant children\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Undocumented immigrant children\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"Government policy\",\"authorized\":false},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"United States\",\"authorized\":false,\"type\":\"Topical Term\"},{\"subject\":\"Emigration and immigration\",\"authorized\":false},{\"subject\":\"Government policy.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Emigration and immigration law\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"United States.\",\"authorized\":true}]\n" + 
//		"subject_json: [{\"subject\":\"POLITICAL SCIENCE\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"American Government.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Immigration law\",\"authorized\":false,\"type\":\"Topical Term\"},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 

		"subject_display: Undocumented immigrants > United States\n" + 
		"subject_display: Undocumented immigrants > Government policy > United States\n" + 
		"subject_display: Undocumented immigrant children > United States\n" + 
		"subject_display: Undocumented immigrant children > Government policy > United States\n" + 
		"subject_display: United States > Emigration and immigration > Government policy\n" + 
		"subject_display: Emigration and immigration law > United States\n" + 
//		"subject_display: POLITICAL SCIENCE > American Government\n" + 
		"subject_display: Immigration law > United States\n" + 

		"authority_subject_t: Law, Immigration\n" + 
		"authority_subject_t: Administration\n" + 
		"authority_subject_t: Political theory\n" + 
		"authority_subject_t: Emigration and immigration > Law and legislation\n" + 
		"authority_subject_t: Illegal aliens.\n" + 
		"authority_subject_t: United States > Emigration and immigration law\n" + 
		"authority_subject_t: Government\n" + 
		"authority_subject_t: Illegal aliens > Legal status, laws, etc.\n" + 
		"authority_subject_t: Immigration law\n" + 
		"authority_subject_t: Immigrants > Legal status, laws, etc.\n" + 
		"authority_subject_t: Law, Emigration\n" + 
		"authority_subject_t: Illegal immigration\n" + 
		"authority_subject_t: Illegal aliens\n" + 
		"authority_subject_t: Illegal immigrants\n" + 
		"authority_subject_t: Aliens, Illegal\n" + 
		"authority_subject_t: Civil government\n" + 
		"authority_subject_t: Science, Political\n" + 
		"authority_subject_t: Undocumented children\n" + 
		"authority_subject_t: Commonwealth, The\n" + 
		"authority_subject_t: Political thought\n" + 
		"authority_subject_t: Aliens > Legal status, laws, etc.\n" + 
		"authority_subject_t: Politics\n" + 
		"authority_subject_t: Undocumented aliens\n" + 
		"authority_subject_t: Illegal alien children\n" + 
		"fast_b: true\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test // Should not populate vocabulary-specific fields intended for authority control
	// And, now, also should not populate display and public facet fields due to untracked vocab
	public void cjkIn6xx() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','7',"‡a 子部 ‡x 小說家類. ‡2 sk"));
		String expected =
		"subject_t: 子部 > 小說家類\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void matching880withwrong2ndIndicator() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "5071114";
		rec.dataFields.add(new DataField(1,4,"600",'1','0',"‡6 880-06 ‡a Zhang, Lei, ‡d 1054-1114 ‡v Chronology.",false));
		rec.dataFields.add(new DataField(2,4,"600",'1','4',"‡6 600-06/$1 ‡a 張耒, ‡d 1054-1114 ‡v Chronology.",true));
		/* This name actually does have an authority record, but we have other tests to capture that, so
		 * I saw no need to import that into the test suite just to get 9 alternate forms of the name adding
		 * to the expected Solr results.
		 */
		String expected =
		"subject_t: 張耒, 1054-1114 > Chronology\n"+
		"subject_t: Zhang, Lei, 1054-1114 > Chronology\n"+
		"subject_pers_facet: 張耒, 1054-1114\n"+
		"subject_pers_filing: 張耒 1054 1114\n"+
		"subject_pers_facet: 張耒, 1054-1114 > Chronology\n"+
		"subject_pers_filing: 張耒 1054 1114 0000 chronology\n"+
		"subject_pers_facet: Zhang, Lei, 1054-1114\n"+
		"subject_pers_filing: zhang lei 1054 1114\n"+
		"subject_pers_lc_facet: Zhang, Lei, 1054-1114\n"+
		"subject_pers_lc_filing: zhang lei 1054 1114\n"+
		"subject_pers_facet: Zhang, Lei, 1054-1114 > Chronology\n"+
		"subject_pers_filing: zhang lei 1054 1114 0000 chronology\n"+
		"subject_pers_lc_facet: Zhang, Lei, 1054-1114 > Chronology\n"+
		"subject_pers_lc_filing: zhang lei 1054 1114 0000 chronology\n"+
		"subject_sub_lc_facet: Chronology\n"+
		"subject_sub_lc_filing: chronology\n"+
		"subject_json: [{\"subject\":\"張耒, 1054-1114\",\"authorized\":false,\"type\":\"Personal Name\"},"
		+ "{\"subject\":\"Chronology.\",\"authorized\":false}]\n"+
		"subject_json: [{\"subject\":\"Zhang, Lei, 1054-1114\",\"authorized\":false,\"type\":\"Personal Name\"},"
		+ "{\"subject\":\"Chronology.\",\"authorized\":false}]\n"+
		"subject_display: 張耒, 1054-1114 > Chronology\n"+
		"subject_display: Zhang, Lei, 1054-1114 > Chronology\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test //Only the term with an unrecognized but specified vocab should be suppressed from display.
	public void undisplayedVocabularies() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"650",' ','7',"‡2 unrec ‡a Unrecognized Vocab Term"));
		rec.dataFields.add(new DataField(2,"650",' ','7',"‡2 aat ‡a Recognized Vocab Term"));
		rec.dataFields.add(new DataField(3,"650",' ','4',"‡a Unspecified Vocab Term"));
		rec.dataFields.add(new DataField(4,"650",' ','7',"‡a Unspecified Vocab Term B"));
		rec.dataFields.add(new DataField(5,"650",' ','7',"‡a Rare & Manuscript Term ‡2 rbmscv"));
		rec.dataFields.add(new DataField(6,"650",' ','7',"‡2 zst ‡a Zine Vocab Term"));
		String expected =
		"subject_t: Unrecognized Vocab Term\n" +
		"subject_topic_other_facet: Unrecognized Vocab Term\n" +
		"subject_topic_other_filing: unrecognized vocab term\n" +

		"subject_t: Recognized Vocab Term\n" +
		"subject_topic_facet: Recognized Vocab Term\n" +
		"subject_topic_filing: recognized vocab term\n" +
		"subject_topic_aat_facet: Recognized Vocab Term\n" +
		"subject_topic_aat_filing: recognized vocab term\n" +

		"subject_t: Unspecified Vocab Term\n" +
		"subject_topic_facet: Unspecified Vocab Term\n" +
		"subject_topic_filing: unspecified vocab term\n" +
		"subject_topic_unk_facet: Unspecified Vocab Term\n" +
		"subject_topic_unk_filing: unspecified vocab term\n" +

		"subject_t: Unspecified Vocab Term B.\n" +
		"subject_topic_facet: Unspecified Vocab Term B.\n" +
		"subject_topic_filing: unspecified vocab term b\n" +
		"subject_topic_unk_facet: Unspecified Vocab Term B.\n" +
		"subject_topic_unk_filing: unspecified vocab term b\n" +

		"subject_t: Rare & Manuscript Term\n" +
		"subject_topic_facet: Rare & Manuscript Term\n" +
		"subject_topic_filing: rare manuscript term 5&\n" +
		"subject_topic_rbmscv_facet: Rare & Manuscript Term\n" +
		"subject_topic_rbmscv_filing: rare manuscript term 5&\n" +

		"subject_t: Zine Vocab Term\n" +
		"subject_topic_facet: Zine Vocab Term\n" +
		"subject_topic_filing: zine vocab term\n" +
		"subject_topic_zst_facet: Zine Vocab Term\n" +
		"subject_topic_zst_filing: zine vocab term\n" +

		"subject_json: [{\"subject\":\"Recognized Vocab Term\",\"authorized\":false,\"type\":\"Topical Term\"}]\n" +
		"subject_json: [{\"subject\":\"Unspecified Vocab Term\",\"authorized\":false,\"type\":\"Topical Term\"}]\n" +
		"subject_json: [{\"subject\":\"Unspecified Vocab Term B\",\"authorized\":false,\"type\":\"Topical Term\"}]\n" +
		"subject_json: [{\"subject\":\"Rare & Manuscript Term\",\"authorized\":false,\"type\":\"Topical Term\"}]\n"+
		"subject_json: [{\"subject\":\"Zine Vocab Term\",\"authorized\":false,\"type\":\"Topical Term\"}]\n" +
		"subject_display: Recognized Vocab Term\n" +
		"subject_display: Unspecified Vocab Term\n" +
		"subject_display: Unspecified Vocab Term B.\n" +
		"subject_display: Rare & Manuscript Term\n" +
		"subject_display: Zine Vocab Term\n" +
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void ingestVocabulary() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "15607152";
		rec.dataFields.add(new DataField(2,"650",' ','7',"‡a Clothing industry--Study and teaching. ‡2 ingest"));
		rec.dataFields.add(new DataField(7,"650",' ','0',"‡a Interior decoration ‡x Study and teaching."));
		rec.dataFields.add(new DataField(10,"650",' ','7',"‡a Work environment -- Design. ‡2 ingest"));
		String expected =
		"subject_t: Clothing industry--Study and teaching\n"+
		"subject_topic_facet: Clothing industry--Study and teaching\n"+
		"subject_topic_filing: clothing industry 0000 study and teaching\n"+
		"subject_topic_unk_facet: Clothing industry--Study and teaching\n"+
		"subject_topic_unk_filing: clothing industry 0000 study and teaching\n"+

		"subject_t: Interior decoration > Study and teaching\n"+
		"subject_topic_facet: Interior decoration\n"+
		"subject_topic_filing: interior decoration\n"+
		"subject_topic_lc_facet: Interior decoration\n"+
		"subject_topic_lc_filing: interior decoration\n"+
		"subject_topic_facet: Interior decoration > Study and teaching\n"+
		"subject_topic_filing: interior decoration 0000 study and teaching\n"+
		"subject_topic_lc_facet: Interior decoration > Study and teaching\n"+
		"subject_topic_lc_filing: interior decoration 0000 study and teaching\n"+

		"subject_sub_lc_facet: Study and teaching\n"+
		"subject_sub_lc_filing: study and teaching\n"+
		"subject_t: Work environment -- Design\n"+
		"subject_topic_facet: Work environment -- Design\n"+
		"subject_topic_filing: work environment 0000 design\n"+
		"subject_topic_unk_facet: Work environment -- Design\n"+
		"subject_topic_unk_filing: work environment 0000 design\n"+

		"subject_json: [{\"subject\":\"Clothing industry--Study and teaching.\",\"authorized\":false,"
		+ "\"type\":\"Topical Term\"}]\n"+
		"subject_json: [{\"subject\":\"Interior decoration\",\"authorized\":false,\"type\":\"Topical Term\"},"
		+ "{\"subject\":\"Study and teaching.\",\"authorized\":false}]\n"+
		"subject_json: [{\"subject\":\"Work environment -- Design.\",\"authorized\":false,\"type\":\"Topical Term\"}]\n"+
		"subject_display: Clothing industry--Study and teaching\n"+
		"subject_display: Interior decoration > Study and teaching\n"+
		"subject_display: Work environment -- Design\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}
}
