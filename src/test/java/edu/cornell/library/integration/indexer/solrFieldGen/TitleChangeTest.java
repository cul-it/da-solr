package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class TitleChangeTest {

	SolrFieldGenerator gen = new TitleChange();
	static Config config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		config = Config.loadConfig(requiredArgs);
	}

	@Test
	public void testSimple700() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"700",'1',' ',"‡a Smith, John, ‡d 1900-1999"));
		String expected = "author_addl_display: Smith, John, 1900-1999\n"+
		"author_addl_t: Smith, John, 1900-1999\n"+
		"author_facet: Smith, John, 1900-1999\n"+
		"author_pers_filing: smith john 1900 1999\n"+
		"author_addl_json: {\"name1\":\"Smith, John, 1900-1999\",\"search1\":\"Smith, John, 1900-1999\","
		+ "\"relator\":\"\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorized700WithRelator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"700",'1',' ',"‡a Ko, Dorothy, ‡d 1957- ‡e author."));
		String expected = "author_addl_display: Ko, Dorothy, 1957- author\n"+
		"author_addl_t: Ko, Dorothy, 1957- author\n"+
		"author_facet: Ko, Dorothy, 1957-\n"+
		"author_pers_filing: ko dorothy 1957\n"+
		"author_addl_json: {\"name1\":\"Ko, Dorothy, 1957- author\",\"search1\":\"Ko, Dorothy, 1957-\","
		+ "\"relator\":\"author\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: Gao, Yanyi, 1957-\n"+
		"authority_author_t: 高彦颐, 1957-\n"+
		"authority_author_t_cjk: 高彦颐, 1957-\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorTitle700() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-1878
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"700",'1','2',"‡a Sallinen, Aulis. ‡t Vintern war hård; ‡o arranged."));
		rec.dataFields.add(new DataField(2,"700",'1','2',"‡a Riley, Terry, ‡d 1935- ‡t Salome"
				+ " dances for peace. ‡p Half-wolf dances mad in moonlight."));
		rec.dataFields.add(new DataField(3,"700",'1','2',"‡a Barber, Samuel, ‡d 1910-1981. ‡t Quartets, ‡m violins"
				+ " (2), viola, cello, ‡n no. 1, op. 11, ‡r B minor. ‡p Adagio."));
		String expected = "authortitle_facet: Sallinen, Aulis. | Vintern war hård; arranged\n"+
		"authortitle_filing: sallinen aulis 0000 vintern war hard arranged\n"+
		"author_addl_t: Sallinen, Aulis.\n"+
		"author_facet: Sallinen, Aulis\n"+
		"author_pers_filing: sallinen aulis\n"+
		"included_work_display: Sallinen, Aulis. Vintern war hård; arranged.|Vintern war hård; arranged.|"
		+ "Sallinen, Aulis.\n"+
		"title_uniform_t: Vintern war hård; arranged.\n"+
		"authortitle_facet: Riley, Terry, 1935- | Salome dances for peace. Half-wolf dances mad in moonlight\n"+
		"authortitle_filing: riley terry 1935 0000 salome dances for peace half wolf dances mad in moonlight\n"+
		"author_addl_t: Riley, Terry, 1935-\n"+
		"author_facet: Riley, Terry, 1935-\n"+
		"author_pers_filing: riley terry 1935\n"+
		"included_work_display: Riley, Terry, 1935- Salome dances for peace. Half-wolf dances mad in moonlight.|"
		+ "Salome dances for peace. Half-wolf dances mad in moonlight.|Riley, Terry, 1935-\n"+
		"title_uniform_t: Salome dances for peace. Half-wolf dances mad in moonlight.\n"+
		"authortitle_facet: Barber, Samuel, 1910-1981. | Quartets, violins (2), viola, cello, no. 1, op. 11,"
		+ " B minor. Adagio\n"+
		"authortitle_filing: barber samuel 1910 1981 0000 quartets violins 2 viola cello no 1 op 11 b minor adagio\n"+
		"author_addl_t: Barber, Samuel, 1910-1981.\n"+
		"author_facet: Barber, Samuel, 1910-1981\n"+
		"author_pers_filing: barber samuel 1910 1981\n"+
		"included_work_display: Barber, Samuel, 1910-1981. Quartets, violins (2), viola, cello, no. 1, op. 11,"
		+ " B minor. Adagio.|Quartets, violins (2), viola, cello, no. 1, op. 11, B minor. Adagio.|"
		+ "Barber, Samuel, 1910-1981.\n"+
		"title_uniform_t: Quartets, violins (2), viola, cello, no. 1, op. 11, B minor. Adagio.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testNonRoman700() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,5,"700",'1',' ',"‡6 880-05 ‡a Xiang, Shurong, ‡e translator.",false));
		rec.dataFields.add(new DataField(2,6,"700",'1',' ',"‡6 880-06 ‡a Yao, Jianing, ‡e translator.",false));
		rec.dataFields.add(new DataField(3,5,"700",'1',' ',"‡6 700-05/$1 ‡a 向淑容, ‡e translator.",true));
		rec.dataFields.add(new DataField(4,6,"700",'1',' ',"‡6 700-06/$1 ‡a 堯嘉寧, ‡e translator.",true));
		String expected =
		"author_addl_display: 向淑容 / Xiang, Shurong, translator\n"+
		"author_facet: 向淑容\n"+
		"author_facet: Xiang, Shurong\n"+
		"author_pers_filing: 向淑容\n"+
		"author_pers_filing: xiang shurong\n"+
		"author_addl_t_cjk: 向淑容, translator\n"+
		"author_addl_t: Xiang, Shurong, translator\n"+
		"author_addl_json: {\"name1\":\"向淑容\",\"search1\":\"向淑容,\",\"name2\":\"Xiang, Shurong, translator\","
		+ "\"search2\":\"Xiang, Shurong,\",\"relator\":\"translator\","
		+ "\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
		"author_addl_display: 堯嘉寧 / Yao, Jianing, translator\n"+
		"author_facet: 堯嘉寧\n"+
		"author_facet: Yao, Jianing\n"+
		"author_pers_filing: 堯嘉寧\n"+
		"author_pers_filing: yao jianing\n"+
		"author_addl_t_cjk: 堯嘉寧, translator\n"+
		"author_addl_t: Yao, Jianing, translator\n"+
		"author_addl_json: {\"name1\":\"堯嘉寧\",\"search1\":\"堯嘉寧,\",\"name2\":\"Yao, Jianing, translator\","
		+ "\"search2\":\"Yao, Jianing,\",\"relator\":\"translator\","
		+ "\"type\":\"Personal Name\",\"authorizedForm\":false}\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testNonRoman710WithRelator() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,6,"710",'2',' ',"‡6 880-06 ‡a Fa lü chu ban she. ‡b Fa gui chu ban"
				+ " fen she, ‡e editor.",false));
		rec.dataFields.add(new DataField(2,6,"710",'2',' ',"‡6 710-06/$1 ‡a 法律出版社. ‡b 法规出版分社, ‡e editor.",true));
		String expected = "author_addl_display: 法律出版社. 法规出版分社 / Fa lü chu ban she. Fa gui chu ban fen she, editor\n"+
		"author_facet: 法律出版社. 法规出版分社\n"+
		"author_facet: Fa lü chu ban she. Fa gui chu ban fen she\n"+
		"author_corp_filing: 法律出版社 法规出版分社\n"+
		"author_corp_filing: fa lu chu ban she fa gui chu ban fen she\n"+
		"author_addl_t_cjk: 法律出版社. 法规出版分社, editor\n"+
		"author_addl_t: Fa lü chu ban she. Fa gui chu ban fen she, editor\n"+
		"author_addl_json: {\"name1\":\"法律出版社. 法规出版分社\",\"search1\":\"法律出版社. 法规出版分社,\","
		+ "\"name2\":\"Fa lü chu ban she. Fa gui chu ban fen she, editor\",\"search2\":"
		+ "\"Fa lü chu ban she. Fa gui chu ban fen she,\",\"relator\":\"editor\","
		+ "\"type\":\"Corporate Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: 法律出版社. 法规出版分社.\n"+
		"authority_author_t_cjk: 法律出版社. 法规出版分社.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test730WithSubfieldI() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-3496
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"730",'0','2',"‡i Container of (work): ‡a All the way (Television program)"));
		String expected = "title_uniform_t: All the way (Television program)\n"+
		"included_work_display: Container of (work): All the way (Television program)|All the way (Television program)\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test740RelatedWork() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"740",'0',' ',"‡a Historic structure report. ‡p Architectural data section. ‡n Phase II, ‡p Exterior preservation."));
		String expected =
		"title_addl_t: Historic structure report. Architectural data section. Phase II, Exterior preservation.\n"+
		"related_work_display: Historic structure report. Architectural data section."
		+ " Phase II, Exterior preservation.|Historic structure report.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
//		System.out.println( gen.generateSolrFields(rec, config).toString().replaceAll("\"","\\\\\"") );
	}

	@Test
	public void testAuthorTitleSegregationOf776() throws ClassNotFoundException, SQLException, IOException {
		{ // Example from DISCOVERYACCESS-3445 b10047079
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'0','8',
					"‡i Print version: "
					+ "‡a Rosengarten, Frank, 1927- "
					+ "‡t Revolutionary Marxism of Antonio Gramsci. "
					+ "‡d Leiden, Netherlands : Brill, c2013 "
					+ "‡h viii, 197 pages "
					+ "‡k Historical materialism book series ; Volume 62. "
					+ "‡x 1570-1522 "
					+ "‡z 9789004265745 "
					+ "‡w 2013041807"));
			String expected =
			"title_uniform_t:"
			+ " Revolutionary Marxism of Antonio Gramsci."
			+ " Leiden, Netherlands : Brill, c2013"
			+ " Historical materialism book series ; Volume 62.\n" + 
			"other_form_display:"
			+ " Print version:"
			+ " Rosengarten, Frank, 1927-"
			+ " | Revolutionary Marxism of Antonio Gramsci."
			+ " Leiden, Netherlands : Brill, c2013"
			+ " Historical materialism book series ; Volume 62."
			+ " viii, 197 pages"
			+ " ISSN: 1570-1522,"
			+ " ISBN: 9789004265745\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
		}
		{ // 7655324
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'0','8',
					"‡a United States. "
					+ "‡b Congress "
					+ "‡n (111th, 1st session : "
					+ "‡d 2009). "
					+ "‡t Concurrent resolution on the budget for fiscal year 2010 "
					+ "‡h 149 p. "
					+ "‡w (OCoLC)320776469"));
			String expected =
			"title_uniform_t:"
			+ " Concurrent resolution on the budget for fiscal year 2010\n" + 
			"other_form_display:"
			+ " United States. Congress (111th, 1st session : 2009)."
			+ " | Concurrent resolution on the budget for fiscal year 2010"
			+ " 149 p."
			+ " (OCoLC)320776469\n";
			assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
		}
	}

	@Test
	public void testNonBib776Fields() throws ClassNotFoundException, SQLException, IOException {
		{ // b 9926193
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'0','8',"‡i Original: ‡w (Voyager)3605552"));
			assertEquals(
					"other_form_display: Original: (Voyager)3605552\n",
					this.gen.generateSolrFields(rec, config).toString() );
		}
		{ // b 10646825
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'1',' ',
					"‡c Original publisher catalog number ‡o 724355800626"));
			assertEquals(
					"other_form_display: Original publisher catalog number 724355800626\n",
					this.gen.generateSolrFields(rec, config).toString() );
		}
		{ // b 8599594
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'1',' ',"‡x 2234-3164"));
			assertEquals(
					"other_form_display: ISSN: 2234-3164\n",
					this.gen.generateSolrFields(rec, config).toString() );
		}
		{ // b 10705523 unqualified identifier suppressed from display
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'1',' ',"‡o 5161733129"));
			assertEquals( "", this.gen.generateSolrFields(rec, config).toString() );
		}
		{ // b 10205060
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.dataFields.add(new DataField(1,"776",'0','8',
					"‡z 9781315116143 ‡z 9781351652728 ‡z 9781351648110 ‡z 9781351638531"));
			assertEquals(
					"other_form_display: ISBN: 9781315116143, ISBN: 9781351652728,"
					+ " ISBN: 9781351648110, ISBN: 9781351638531\n",
					this.gen.generateSolrFields(rec, config).toString() );
		}
	}

	@Test
	public void test711AuthorTitle() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-2492
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"711",'2','2',"‡a Vatican Council ‡n (2nd : ‡d 1962-1965). ‡t"
				+ " Constitutio pastoralis de ecclesia in mundo huius temporis ‡n Nn. 19-21. ‡l English."));
		String expected =
		"authortitle_facet: Vatican Council (2nd : 1962-1965). | Constitutio pastoralis de ecclesia"
		+ " in mundo huius temporis Nn. 19-21. English\n"+
		"authortitle_filing: vatican council 2nd 1962 1965 0000 constitutio pastoralis de ecclesia in mundo"
		+ " huius temporis nn 19 21 english\n"+
		"author_addl_t: Vatican Council (2nd : 1962-1965).\n"+
		"author_facet: Vatican Council\n"+
		"author_event_filing: vatican council\n"+
		"included_work_display: Vatican Council (2nd : 1962-1965). Constitutio pastoralis de ecclesia in"
		+ " mundo huius temporis Nn. 19-21. English.|Constitutio pastoralis de ecclesia in mundo huius"
		+ " temporis Nn. 19-21. English.|Vatican Council (2nd : 1962-1965).\n"+
		"title_uniform_t: Constitutio pastoralis de ecclesia in mundo huius temporis Nn. 19-21. English.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test711Author() throws ClassNotFoundException, SQLException, IOException {
		// Example from DISCOVERYACCESS-2492
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"711",'2','0',"‡a Institute on Religious Freedom ‡d (1966 : ‡c"
				+ " North Aurora, Ill.)"));
		String expected =
		"author_addl_display: Institute on Religious Freedom (1966 : North Aurora, Ill.)\n"+
		"author_addl_t: Institute on Religious Freedom (1966 : North Aurora, Ill.)\n"+
		"author_facet: Institute on Religious Freedom\n"+
		"author_event_filing: institute on religious freedom\n"+
		"author_addl_json: {\"name1\":\"Institute on Religious Freedom (1966 : North Aurora, Ill.)\","
		+ "\"search1\":\"Institute on Religious Freedom (1966 : North Aurora, Ill.)\",\"relator\":\"\","
		+ "\"type\":\"Event\",\"authorizedForm\":false}\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test720() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"720",' ',' ',"‡a al-Salimi, Abdulrahman"));
		String expected =
		"author_addl_display: al-Salimi, Abdulrahman\n"+
		"author_addl_t: al-Salimi, Abdulrahman\n"+
		"author_facet: al-Salimi, Abdulrahman\n"+
		"author_addl_json: {\"name1\":\"al-Salimi, Abdulrahman\",\"search1\":\"al-Salimi, Abdulrahman\","
		+ "\"relator\":\"\",\"authorizedForm\":false}\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test2684613() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(3,"776",'1',' ',
				"‡a In vitro cellular & developmental biology. ‡p Animal (Online)"));
		String expected =
		"title_uniform_t: Animal (Online)\n"+
		"other_form_display: In vitro cellular & developmental biology. | Animal (Online)\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void test2812927() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(2,"700",'1',' ', "‡s Schnoor, Jerald A."));
		assertEquals( "", this.gen.generateSolrFields(rec, config).toString() );
	}	
}
