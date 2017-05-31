package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class TitleChangeTest {

	static SolrBuildConfig config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
	}

	@Test
	public void testSimple700() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"700",'1',' ',"‡a Smith, John, ‡d 1900-1999"));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("author_addl_display", "Smith, John, 1900-1999"),
				new SolrField("author_addl_t",       "Smith, John, 1900-1999"),
				new SolrField("author_addl_cts",     "Smith, John, 1900-1999|Smith, John, 1900-1999"),
				new SolrField("author_facet",        "Smith, John, 1900-1999"),
				new SolrField("author_pers_filing",  "smith john 1900 1999"),
				new SolrField("author_addl_json","{\"name1\":\"Smith, John, 1900-1999\",\"search1\":"
						+ "\"Smith, John, 1900-1999\",\"type\":\"Personal Name\",\"authorizedForm\":false}"));
//		for (SolrField sf : TitleChange.generateSolrFields(rec, config).fields)
//			System.out.println(sf.fieldName+": "+sf.fieldValue);
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

	@Test
	public void testAuthorized700WithRelator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"700",'1',' ',"‡a Ko, Dorothy, ‡d 1957- ‡e author."));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("author_addl_display",    "Ko, Dorothy, 1957- author"),
				new SolrField("author_addl_t",          "Ko, Dorothy, 1957- author"),
				new SolrField("author_addl_cts",        "Ko, Dorothy, 1957- author|Ko, Dorothy, 1957-"),
				new SolrField("author_facet",           "Ko, Dorothy, 1957-"),
				new SolrField("author_pers_filing",     "ko dorothy 1957"),
				new SolrField("author_addl_json","{\"name1\":\"Ko, Dorothy, 1957- author\",\"search1\":"
						+ "\"Ko, Dorothy, 1957-\",\"type\":\"Personal Name\",\"authorizedForm\":true}"),
				new SolrField("authority_author_t",     "Gao, Yanyi, 1957-"),
				new SolrField("authority_author_t",     "高彦颐, 1957-"),
				new SolrField("authority_author_t_cjk", "高彦颐, 1957-"));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
		
	}

	@Test
	public void testNonRoman700() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,5,"700",'1',' ',"‡6 880-05 ‡a Xiang, Shurong, ‡e translator.",false));
		rec.dataFields.add(new DataField(2,6,"700",'1',' ',"‡6 880-06 ‡a Yao, Jianing, ‡e translator.",false));
		rec.dataFields.add(new DataField(3,5,"700",'1',' ',"‡6 700-05/$1 ‡a 向淑容, ‡e translator.",true));
		rec.dataFields.add(new DataField(4,6,"700",'1',' ',"‡6 700-06/$1 ‡a 堯嘉寧, ‡e translator.",true));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("author_addl_display", "向淑容 / Xiang, Shurong, translator"),
				new SolrField("author_addl_cts",     "向淑容|向淑容,|Xiang, Shurong, translator|Xiang, Shurong,"),
				new SolrField("author_facet",        "向淑容"),
				new SolrField("author_facet",        "Xiang, Shurong"),
				new SolrField("author_pers_filing",  "向淑容"),
				new SolrField("author_pers_filing",  "xiang shurong"),
				new SolrField("author_addl_t_cjk",   "向淑容, translator"),
				new SolrField("author_addl_t",       "Xiang, Shurong, translator"),
				new SolrField("author_addl_json","{\"name1\":\"向淑容\",\"search1\":\"向淑容,\",\"name2\":"
						+ "\"Xiang, Shurong, translator\",\"search2\":\"Xiang, Shurong,\",\"type\":"
						+ "\"Personal Name\",\"authorizedForm\":false}"),
				new SolrField("author_addl_display", "堯嘉寧 / Yao, Jianing, translator"),
				new SolrField("author_addl_cts",     "堯嘉寧|堯嘉寧,|Yao, Jianing, translator|Yao, Jianing,"),
				new SolrField("author_facet",        "堯嘉寧"),
				new SolrField("author_facet",        "Yao, Jianing"),
				new SolrField("author_pers_filing",  "堯嘉寧"),
				new SolrField("author_pers_filing",  "yao jianing"),
				new SolrField("author_addl_t_cjk",   "堯嘉寧, translator"),
				new SolrField("author_addl_t",       "Yao, Jianing, translator"),
				new SolrField("author_addl_json","{\"name1\":\"堯嘉寧\",\"search1\":\"堯嘉寧,\",\"name2\":"
						+ "\"Yao, Jianing, translator\",\"search2\":\"Yao, Jianing,\",\"type\":"
						+ "\"Personal Name\",\"authorizedForm\":false}"));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	
	}

	@Test
	public void testNonRoman710WithRelator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,6,"710",'2',' ',"‡6 880-06 ‡a Fa lü chu ban she. ‡b Fa gui chu ban"
				+ " fen she, ‡e editor.",false));
		rec.dataFields.add(new DataField(2,6,"710",'2',' ',"‡6 710-06/$1 ‡a 法律出版社. ‡b 法规出版分社, ‡e editor.",true));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("author_addl_display", "法律出版社. 法规出版分社 / Fa lü chu ban she. Fa gui"
						+ " chu ban fen she, editor"),
				new SolrField("author_addl_cts",     "法律出版社. 法规出版分社|法律出版社. 法规出版分社,|Fa lü"
						+ " chu ban she. Fa gui chu ban fen she, editor|Fa lü chu ban she. Fa gui chu ban fen she,"),
				new SolrField("author_facet",        "法律出版社. 法规出版分社"),
				new SolrField("author_facet",        "Fa lü chu ban she. Fa gui chu ban fen she"),
				new SolrField("author_corp_filing",  "法律出版社 法规出版分社"),
				new SolrField("author_corp_filing",  "fa lu chu ban she fa gui chu ban fen she"),
				new SolrField("author_addl_t_cjk",   "法律出版社. 法规出版分社, editor"),
				new SolrField("author_addl_t",       "Fa lü chu ban she. Fa gui chu ban fen she, editor"),
				new SolrField("author_addl_json","{\"name1\":\"法律出版社. 法规出版分社\",\"search1\":"
						+ "\"法律出版社. 法规出版分社,\",\"name2\":\"Fa lü chu ban she. Fa gui chu "
						+ "ban fen she, editor\",\"search2\":\"Fa lü chu ban she. Fa gui chu ban fen she,\","
						+ "\"type\":\"Corporate Name\",\"authorizedForm\":false}"));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

	@Test
	public void test730WithSubfieldI() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-3496
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"730",'0','2',"‡i Container of (work): ‡a All the way (Television program)"));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("title_uniform_t",       "All the way (Television program)"),
				new SolrField("included_work_display",
						"Container of (work): All the way (Television program)|All the way (Television program)"));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

	@Test
	public void testAuthorTitleSegregationOf776() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-3445
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"776",'0','8',"‡i Print version: ‡a Rosengarten, Frank, 1927- ‡t"
				+ " Revolutionary Marxism of Antonio Gramsci. ‡d Leiden, Netherlands : Brill, c2013 ‡h viii,"
				+ " 197 pages ‡k Historical materialism book series ; Volume 62. ‡x 1570-1522 ‡z 9789004265745"
				+ " ‡w 2013041807"));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("title_uniform_t",
						"Revolutionary Marxism of Antonio Gramsci. Leiden, Netherlands : Brill, c2013 Historical"
						+ " materialism book series ; Volume 62."),
				new SolrField("other_form_display",
						"Print version: Rosengarten, Frank, 1927- | Revolutionary Marxism of Antonio Gramsci."
						+ " Leiden, Netherlands : Brill, c2013 Historical materialism book series ; Volume 62."));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

	@Test
	public void test711AuthorTitle() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-2492
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"711",'2','2',"‡a Vatican Council ‡n (2nd : ‡d 1962-1965). ‡t"
				+ " Constitutio pastoralis de ecclesia in mundo huius temporis ‡n Nn. 19-21. ‡l English."));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("authortitle_facet",
						"Vatican Council (2nd : 1962-1965). | Constitutio pastoralis de ecclesia in mundo huius"
						+ " temporis Nn. 19-21. English"),
				new SolrField("authortitle_filing",
						"vatican council 2nd 1962 1965 0000 constitutio pastoralis de ecclesia in mundo huius"
						+ " temporis nn 19 21 english"),
				new SolrField("author_addl_t",
						"Vatican Council (2nd : 1962-1965)."),
				new SolrField("author_facet",
						"Vatican Council"),
				new SolrField("author_event_filing",
						"vatican council"),
				new SolrField("included_work_display",
						"Vatican Council (2nd : 1962-1965). Constitutio pastoralis de ecclesia in mundo huius"
						+ " temporis Nn. 19-21. English.|Constitutio pastoralis de ecclesia in mundo huius"
						+ " temporis Nn. 19-21. English.|Vatican Council (2nd : 1962-1965)."),
				new SolrField("title_uniform_t",
						"Constitutio pastoralis de ecclesia in mundo huius temporis Nn. 19-21. English."));
//		for (SolrField sf : TitleChange.generateSolrFields(rec, config).fields)
//			System.out.println(sf.fieldName+": "+sf.fieldValue);
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

	@Test
	public void test711Author() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-2492
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(new DataField(1,"711",'2','0',"‡a Institute on Religious Freedom ‡d (1966 : ‡c"
				+ " North Aurora, Ill.)"));
		SolrFields expected = new SolrFields();
		expected.fields = Arrays.asList(
				new SolrField("author_addl_display",
						"Institute on Religious Freedom (1966 : North Aurora, Ill.)"),
				new SolrField("author_addl_t",
						"Institute on Religious Freedom (1966 : North Aurora, Ill.)"),
				new SolrField("author_addl_cts",
						"Institute on Religious Freedom (1966 : North Aurora, Ill.)|Institute on Religious Freedom"
						+ " (1966 : North Aurora, Ill.)"),
				new SolrField("author_facet",
						"Institute on Religious Freedom"),
				new SolrField("author_event_filing",
						"institute on religious freedom"),
				new SolrField("author_addl_json",
						"{\"name1\":\"Institute on Religious Freedom (1966 : North Aurora, Ill.)\",\"search1\":"
						+ "\"Institute on Religious Freedom (1966 : North Aurora, Ill.)\",\"type\":\"Event\","
						+ "\"authorizedForm\":false}"));
		assertTrue(expected.equals(TitleChange.generateSolrFields(rec, config)));
	}

}
