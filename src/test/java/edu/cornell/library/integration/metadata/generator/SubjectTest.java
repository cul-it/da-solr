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
	public static void setup() throws IOException {
		setup("Headings");
	}

	@Test
	public void testAuthorizedNoFAST() throws ClassNotFoundException, SQLException, IOException {
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
	public void testAuthorizedWithFAST() throws ClassNotFoundException, SQLException, IOException {
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
	public void testChronFAST() throws ClassNotFoundException, SQLException, IOException {
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
	public void testChronFASTWithUnwantedSpaces() throws ClassNotFoundException, SQLException, IOException {
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
	public void testComplex610() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"610",'2','0',"‡a Jesuits. ‡b Congregatio Generalis ‡n (32nd :"
				+ " ‡d 1974-1975 : ‡c Rome, Italy). ‡t Decree Four."));
		String expected =
		"subject_t: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four\n"+
		"subject_work_lc_facet: Jesuits. Congregatio Generalis (32nd : 1974-1975 : Rome, Italy). | Decree Four\n"+
		"subject_work_lc_filing: jesuits congregatio generalis 32nd 1974 1975 rome italy 0000 decree four\n"+
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
		"subject_json: [{\"subject\":\"Bible. | Paraphrases. O.T. English.\","
		+ "\"authorized\":false,\"type\":\"Work\"}]\n"+
		"subject_display: Bible. | Paraphrases. O.T. English\n"+
		"fast_b: false\n";
		assertEquals(expected,this.gen.generateSolrFields(rec, config).toString());
	}

	@Test
	public void testNonRoman610() throws ClassNotFoundException, SQLException, IOException {
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
	public void testUnwantedFacetValue() throws ClassNotFoundException, SQLException, IOException {
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
	public void test653() throws ClassNotFoundException, SQLException, IOException {
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
	public void test653core() throws ClassNotFoundException, SQLException, IOException {
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
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "10215428";
		rec.dataFields.add(new DataField(1,"651",' ','7',"‡a Cambodia ‡z Svay Riĕng. ‡2 fast ‡0 (OCoLC)fst01878040"));
		assertFalse(this.gen.generateSolrFields(rec, config).toString().contains("Coalition"));
	}

	@Test
	public void swappedOffensiveSubjectTerms() throws ClassNotFoundException, SQLException, IOException {
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

		"subject_t: POLITICAL SCIENCE > American Government\n"+ 
		"subject_topic_facet: POLITICAL SCIENCE\n" +
		"subject_topic_filing: political science\n" +
		"subject_topic_other_facet: POLITICAL SCIENCE\n" +
		"subject_topic_other_filing: political science\n"+ 
		"subject_topic_facet: POLITICAL SCIENCE > American Government\n"+ 
		"subject_topic_filing: political science 0000 american government\n"+ 
		"subject_topic_other_facet: POLITICAL SCIENCE > American Government\n"+ 
		"subject_topic_other_filing: political science 0000 american government\n"+ 
		"subject_sub_other_facet: American Government\n"+
		"subject_sub_other_filing: american government\n"+

		"subject_t: Undocumented immigrants > United States\n" +
		"subject_topic_facet: Undocumented immigrants\n" +
		"subject_topic_filing: undocumented immigrants\n" + 
		"subject_topic_other_facet: Illegal aliens\n" + 
		"subject_topic_other_filing: illegal aliens\n" + 
		"subject_topic_facet: Undocumented immigrants > United States\n" + 
		"subject_topic_filing: undocumented immigrants 0000 united states\n" + 
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
		"subject_json: [{\"subject\":\"POLITICAL SCIENCE\",\"authorized\":true,\"type\":\"Topical Term\"},{\"subject\":\"American Government.\",\"authorized\":false}]\n" + 
		"subject_json: [{\"subject\":\"Immigration law\",\"authorized\":false,\"type\":\"Topical Term\"},{\"subject\":\"United States.\",\"authorized\":false}]\n" + 

		"subject_display: Undocumented immigrants > United States\n" + 
		"subject_display: Undocumented immigrants > Government policy > United States\n" + 
		"subject_display: Undocumented immigrant children > United States\n" + 
		"subject_display: Undocumented immigrant children > Government policy > United States\n" + 
		"subject_display: United States > Emigration and immigration > Government policy\n" + 
		"subject_display: Emigration and immigration law > United States\n" + 
		"subject_display: POLITICAL SCIENCE > American Government\n" + 
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
}
