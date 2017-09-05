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
public class AuthorTitleTest {

	static SolrBuildConfig config = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Headings");
		config = SolrBuildConfig.loadConfig(null,requiredArgs);
	}

	@Test
	public void testMainTitleNoAuthor() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"245",'1','4',"‡a The national law journal"));
		String expected =
		"title_sort: national law journal\n"+
		"title_display: The national law journal\n"+
		"subtitle_display: \n"+
		"fulltitle_display: The national law journal\n"+
		"title_t: The national law journal\n"+
		"title_t: national law journal\n"+
		"title_exact: The national law journal\n"+
		"title_exact: national law journal\n"+
		"title_sms_compat_display: The national law journal\n"+
		"title_2letter_s: na\n"+
		"title_1letter_s: n\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testSimpleAuthorTitle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a León Cupe, Mariano, ‡d 1932-"));
		rec.dataFields.add(new DataField(2,"245",'1','0',"‡a Cabana, historia, cultura y tradición / ‡c Mariano"
				+ " León Cupe, Jorge León Quispe."));
		String expected =
		"author_display: León Cupe, Mariano, 1932-\n"+
		"author_t: León Cupe, Mariano, 1932-\n"+
		"author_cts: León Cupe, Mariano, 1932-|León Cupe, Mariano, 1932-\n"+
		"author_facet: León Cupe, Mariano, 1932-\n"+
		"author_pers_filing: leon cupe mariano 1932\n"+
		"author_json: {\"name1\":\"León Cupe, Mariano, 1932-\",\"search1\":\"León Cupe, Mariano, 1932-\","+
									"\"relator\":\"\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
		"author_sort: leon cupe mariano 1932\n"+
		"title_sort: cabana historia cultura y tradicion\n"+
		"title_display: Cabana, historia, cultura y tradición\n"+
		"subtitle_display: \n"+
		"fulltitle_display: Cabana, historia, cultura y tradición\n"+
		"title_t: Cabana, historia, cultura y tradición\n"+
		"title_t: Cabana, historia, cultura y tradición\n"+
		"title_exact: Cabana, historia, cultura y tradición\n"+
		"title_exact: Cabana, historia, cultura y tradición\n"+
		"title_sms_compat_display: Cabana, historia, cultura y tradicion\n"+
		"title_2letter_s: ca\n"+
		"title_1letter_s: c\n"+
		"authortitle_facet: León Cupe, Mariano, 1932- | Cabana, historia, cultura y tradición\n"+
		"authortitle_filing: leon cupe mariano 1932 0000 cabana historia cultura y tradicion\n"+
		"title_responsibility_display: Mariano León Cupe, Jorge León Quispe.\n"+
		"author_245c_t: Mariano León Cupe, Jorge León Quispe.\n";
//		System.out.println( AuthorTitle.generateSolrFields(rec, config).toString().replaceAll("\"","\\\\\"") );
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorizedAuthorTitle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Fewer, T. N."));
		rec.dataFields.add(new DataField(2,"245",'1','0',"‡a Waterford people : ‡b a biographical dictionary / "
				+ "‡c T. N. Fewer."));
		String expected =
		"author_display: Fewer, T. N.\n"+
		"author_t: Fewer, T. N.\n"+
		"author_cts: Fewer, T. N.|Fewer, T. N.\n"+
		"author_facet: Fewer, T. N.\n"+
		"author_pers_filing: fewer t n\n"+
		"author_json: {\"name1\":\"Fewer, T. N.\",\"search1\":\"Fewer, T. N.\",\"relator\":\"\",\"type\":"+
										"\"Personal Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: Fewer, Tom\n"+
		"author_sort: fewer t n\n"+
		"title_sort: waterford people a biographical dictionary\n"+
		"title_display: Waterford people\n"+
		"subtitle_display: a biographical dictionary\n"+
		"fulltitle_display: Waterford people : a biographical dictionary\n"+
		"title_t: Waterford people : a biographical dictionary\n"+
		"title_t: Waterford people : a biographical dictionary\n"+
		"title_exact: Waterford people : a biographical dictionary\n"+
		"title_exact: Waterford people : a biographical dictionary\n"+
		"title_main_exact: Waterford people\n"+
		"title_main_exact: Waterford people\n"+
		"title_sms_compat_display: Waterford people\n"+
		"title_2letter_s: wa\n"+
		"title_1letter_s: w\n"+
		"authortitle_facet: Fewer, T. N. | Waterford people\n"+
		"authortitle_filing: fewer t n 0000 waterford people\n"+
		"title_responsibility_display: T. N. Fewer.\n"+
		"author_245c_t: T. N. Fewer.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorRelatorTitle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Kalavrezos, Nicholas, ‡u (University"
				+ " College London Hospital, UK) ‡4 spk"));
		rec.dataFields.add(new DataField(2,"245",'1','0',"‡a Lumps and bumps in the mouth and lips"
				+ " ‡h [electronic resource] / ‡c Nicholas Kalavrezos."));
		String expected =
		"author_display: Kalavrezos, Nicholas, speaker\n"+
		"author_t: Kalavrezos, Nicholas, speaker\n"+
		"author_cts: Kalavrezos, Nicholas, speaker|Kalavrezos, Nicholas,\n"+
		"author_facet: Kalavrezos, Nicholas\n"+
		"author_pers_filing: kalavrezos nicholas\n"+
		"author_json: {\"name1\":\"Kalavrezos, Nicholas, speaker\",\"search1\":\"Kalavrezos, Nicholas,\","
		+ "\"relator\":\"speaker\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
		"author_sort: kalavrezos nicholas\n"+
		"title_sort: lumps and bumps in the mouth and lips\n"+
		"title_display: Lumps and bumps in the mouth and lips\n"+
		"subtitle_display: \n"+
		"fulltitle_display: Lumps and bumps in the mouth and lips\n"+
		"title_t: Lumps and bumps in the mouth and lips\n"+
		"title_t: Lumps and bumps in the mouth and lips\n"+
		"title_exact: Lumps and bumps in the mouth and lips\n"+
		"title_exact: Lumps and bumps in the mouth and lips\n"+
		"title_sms_compat_display: Lumps and bumps in the mouth and lips\n"+
		"title_2letter_s: lu\n"+
		"title_1letter_s: l\n"+
		"authortitle_facet: Kalavrezos, Nicholas, | Lumps and bumps in the mouth and lips\n"+
		"authortitle_filing: kalavrezos nicholas 0000 lumps and bumps in the mouth and lips\n"+
		"title_responsibility_display: Nicholas Kalavrezos.\n"+
		"author_245c_t: Nicholas Kalavrezos.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorTitleUniformTitle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Speed, John, ‡d 1552?-1629, ‡e cartographer."));
		rec.dataFields.add(new DataField(2,"240",'1',' ',"‡a Theatre of the empire of Great Britaine"));
		rec.dataFields.add(new DataField(3,"245",'1','0',"‡a Britain's Tudor maps : ‡b county by county /"
				+ " ‡c John Speed ; introduction by Nigel Nicolson ; country commentaries by Alasdair Hawkyard."));
		String expected =
		"author_display: Speed, John, 1552?-1629, cartographer\n"+
		"author_t: Speed, John, 1552?-1629, cartographer\n"+
		"author_cts: Speed, John, 1552?-1629, cartographer|Speed, John, 1552?-1629,\n"+
		"author_facet: Speed, John, 1552?-1629\n"+
		"author_pers_filing: speed john 1552 1629\n"+
		"author_json: {\"name1\":\"Speed, John, 1552?-1629, cartographer\",\"search1\":\"Speed, John, 1552?-1629,\","
		+ "\"relator\":\"cartographer\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: I. S. (John Speed), 1552?-1629\n"+
		"authority_author_t: J. S. (John Speed), 1552?-1629\n"+
		"authority_author_t: S., I. (John Speed), 1552?-1629\n"+
		"authority_author_t: S., J. (John Speed), 1552?-1629\n"+
		"authority_author_t: Spede, Iohn, 1552?-1629\n"+
		"authority_author_t: Speed, I. (John Speed), 1552?-1629\n"+
		"authority_author_t: Speed, Iohn, 1552?-1629\n"+
		"authority_author_t: Speede, Iohn, 1552?-1629\n"+
		"authority_author_t: Speede, John, 1552?-1629\n"+
		"author_sort: speed john 1552 1629\n"+
		"title_uniform_display: Theatre of the empire of Great Britaine|Theatre of the empire of Great"+
									" Britaine|Speed, John, 1552?-1629,\n"+
		"authortitle_facet: Speed, John, 1552?-1629, | Theatre of the empire of Great Britaine\n"+
		"authortitle_filing: speed john 1552 1629 0000 theatre of the empire of great britaine\n"+
		"title_uniform_t: Theatre of the empire of Great Britaine\n"+
		"title_uniform_t: Theatre of the empire of Great Britaine\n"+
		"title_sort: britains tudor maps county by county\n"+
		"title_display: Britain's Tudor maps\n"+
		"subtitle_display: county by county\n"+
		"fulltitle_display: Britain's Tudor maps : county by county\n"+
		"title_t: Britain's Tudor maps : county by county\n"+
		"title_t: Britain's Tudor maps : county by county\n"+
		"title_exact: Britain's Tudor maps : county by county\n"+
		"title_exact: Britain's Tudor maps : county by county\n"+
		"title_main_exact: Britain's Tudor maps\n"+
		"title_main_exact: Britain's Tudor maps\n"+
		"title_sms_compat_display: Britain's Tudor maps\n"+
		"title_2letter_s: br\n"+
		"title_1letter_s: b\n"+
		"title_responsibility_display: John Speed ; introduction by Nigel Nicolson ; country commentaries"+
									" by Alasdair Hawkyard.\n"+
		"author_245c_t: John Speed ; introduction by Nigel Nicolson ; country commentaries by Alasdair Hawkyard.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorTitleWInitialArticle() throws ClassNotFoundException, SQLException, IOException {
		// As described in DISCOVERYACCESS-2972, the second indicator on the title field counts diacritics
		// as characters when describing the length of initial non-sort article.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Papadēmētropoulos, Loukas P., ‡e author."));
		rec.dataFields.add(new DataField(2,"245",'1','4',"‡a Hē ennoia tou oikou ston Euripidē : ‡b Alkēstē, Mēdeia,"
				+ " Hippolytos / ‡c Loukas Papadēmētropoulos."));
		String expected =
		"author_display: Papadēmētropoulos, Loukas P., author\n"+
		"author_t: Papadēmētropoulos, Loukas P., author\n"+
		"author_cts: Papadēmētropoulos, Loukas P., author|Papadēmētropoulos, Loukas P.,\n"+
		"author_facet: Papadēmētropoulos, Loukas P.\n"+
		"author_pers_filing: papademetropoulos loukas p\n"+
		"author_json: {\"name1\":\"Papadēmētropoulos, Loukas P., author\",\"search1\":\"Papadēmētropoulos, Loukas P.,\""
		+ ",\"relator\":\"author\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"author_sort: papademetropoulos loukas p\n"+
		"title_sort: ennoia tou oikou ston euripide alkeste medeia hippolytos\n"+
		"title_display: Hē ennoia tou oikou ston Euripidē\n"+
		"subtitle_display: Alkēstē, Mēdeia, Hippolytos\n"+
		"fulltitle_display: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_t: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_t: ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_exact: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_exact: ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_main_exact: Hē ennoia tou oikou ston Euripidē\n"+
		"title_main_exact: ennoia tou oikou ston Euripidē\n"+
		"title_sms_compat_display: He ennoia tou oikou ston Euripide\n"+
		"title_2letter_s: en\n"+
		"title_1letter_s: e\n"+
		"authortitle_facet: Papadēmētropoulos, Loukas P., | ennoia tou oikou ston Euripidē\n"+
		"authortitle_filing: papademetropoulos loukas p 0000 ennoia tou oikou ston euripide\n"+
		"title_responsibility_display: Loukas Papadēmētropoulos.\n"+
		"author_245c_t: Loukas Papadēmētropoulos.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testNonRomanTitle() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,1,"245",'1','0',"‡6 880-01 ‡a Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta"
				+ " Alekseevna : ‡b perepiska iz trekh uglov 1804-1826 / ‡c podgotovka pisem E. Dmitrievoĭ i F."
				+ " Shedevi.",false));
		rec.dataFields.add(new DataField(2,1,"245",'1','0',"‡6 245-01/(N ‡a Александр I, Мария Павловна, Елизавета"
				+ " Алексеевна : ‡b переписка из трех углов 1804-1826 / ‡c подготовка писем Е. Дмитриевой и Ф."
				+ " Шедеви.",true));
		String expected =
		"title_sort: aleksandr i mariia pavlovna elizaveta alekseevna perepiska iz trekh uglov 1804 1826\n"+
		"title_display: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna\n"+
		"subtitle_display: perepiska iz trekh uglov 1804-1826\n"+
		"fulltitle_display: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna : perepiska iz trekh uglov 1804-1826\n"+
		"title_t: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna : perepiska iz trekh uglov 1804-1826\n"+
		"title_t: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna : perepiska iz trekh uglov 1804-1826\n"+
		"title_exact: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna : perepiska iz trekh uglov 1804-1826\n"+
		"title_exact: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna : perepiska iz trekh uglov 1804-1826\n"+
		"title_main_exact: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna\n"+
		"title_main_exact: Aleksandr I, Marii︠a︡ Pavlovna, Elizaveta Alekseevna\n"+
		"title_sms_compat_display: Aleksandr I, Mariia Pavlovna, Elizaveta Alekseevna\n"+
		"title_2letter_s: al\n"+
		"title_1letter_s: a\n"+
		"title_vern_display: Александр I, Мария Павловна, Елизавета Алексеевна\n"+
		"subtitle_vern_display: переписка из трех углов 1804-1826\n"+
		"title_t: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_t: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"fulltitle_vern_display: Александр I, Мария Павловна, Елизавета Алексеевна : "
		+ "переписка из трех углов 1804-1826\n"+
		"title_exact: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_exact: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_main_exact: Александр I, Мария Павловна, Елизавета Алексеевна\n"+
		"title_main_exact: Александр I, Мария Павловна, Елизавета Алексеевна\n"+
		"title_responsibility_display: подготовка писем Е. Дмитриевой и Ф. Шедеви. /"
		+ " podgotovka pisem E. Dmitrievoĭ i F. Shedevi.\n"+
		"author_245c_t: подготовка писем Е. Дмитриевой и Ф. Шедеви.\n"+
		"author_245c_t: podgotovka pisem E. Dmitrievoĭ i F. Shedevi.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testCJKEverything() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,1,"100",'1',' ',"‡6 880-01 ‡a Taga, Futoshi, ‡d 1968- ‡e author.",false));
		rec.dataFields.add(new DataField(2,2,"240",'1','0',"‡6 880-02 ‡a Danshi mondai no jidai. ‡l Korean",false));
		rec.dataFields.add(new DataField(3,3,"245",'1','0',"‡6 880-03 ‡a Namja munje ŭi sidae = ‡b Danshi mondai"
				+ " no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak / ‡c Taga Hut'osi chiŭm ; Ch'aeksaso omgim.",false));
		rec.dataFields.add(new DataField(4,1,"100",'1',' ',"‡6 100-01/$1 ‡a 多賀太, ‡d 1968- ‡e author.",true));
		rec.dataFields.add(new DataField(5,2,"240",'1','0',"‡6 240-02/$1 ‡a 男子問題の時代. ‡l Korean",true));
		rec.dataFields.add(new DataField(6,3,"245",'1','0',"‡6 245-03/$1 ‡a 남자 문제 의 시대 = ‡b 男子問題の時代?"
				+ " : 젠더 와 교육 의 정치학 / ‡c 다가 후토시 지음 ; 책사소 옮김.",true));
		String expected =
		"author_display: 多賀太 / Taga, Futoshi, 1968- author\n"+
		"author_cts: 多賀太|多賀太, 1968-|Taga, Futoshi, 1968- author|Taga, Futoshi, 1968-\n"+
		"author_facet: 多賀太, 1968-\n"+
		"author_facet: Taga, Futoshi, 1968-\n"+
		"author_pers_filing: 多賀太 1968\n"+
		"author_pers_filing: taga futoshi 1968\n"+
		"author_t_cjk: 多賀太, 1968- author\n"+
		"author_t: Taga, Futoshi, 1968- author\n"+
		"author_json: {\"name1\":\"多賀太\",\"search1\":\"多賀太, 1968-\",\"name2\":\"Taga, Futoshi, 1968- author\","
		+ "\"search2\":\"Taga, Futoshi, 1968-\",\"relator\":\"author\",\"type\":\"Personal Name\","
		+ "\"authorizedForm\":true}\n"+
		"authority_author_t: 多賀太, 1968-\n"+
		"authority_author_t_cjk: 多賀太, 1968-\n"+
		"author_sort: taga futoshi 1968\n"+
		"title_uniform_display: 男子問題の時代. Korean|男子問題の時代. Korean|多賀太, 1968-\n"+
		"authortitle_facet: 多賀太, 1968- | 男子問題の時代. Korean\n"+
		"authortitle_filing: 多賀太 1968 0000 男子問題の時代 korean\n"+
		"title_uniform_t_cjk: 男子問題の時代. Korean\n"+
		"title_uniform_display: Danshi mondai no jidai. Korean|Danshi mondai no jidai. Korean|Taga, Futoshi, 1968-\n"+
		"authortitle_facet: Taga, Futoshi, 1968- | Danshi mondai no jidai. Korean\n"+
		"authortitle_filing: taga futoshi 1968 0000 danshi mondai no jidai korean\n"+
		"title_uniform_t: Danshi mondai no jidai. Korean\n"+
		"title_uniform_t: Danshi mondai no jidai. Korean\n"+
		"title_sort: namja munje ui sidae danshi mondai no jidai chendo wa kyoyuk ui chongchihak\n"+
		"title_display: Namja munje ŭi sidae\n"+
		"subtitle_display: Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"fulltitle_display: Namja munje ŭi sidae = Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"title_t: Namja munje ŭi sidae = Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"title_t: Namja munje ŭi sidae = Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"title_exact: Namja munje ŭi sidae = Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"title_exact: Namja munje ŭi sidae = Danshi mondai no jidai? : chendŏ wa kyoyuk ŭi chŏngch'ihak\n"+
		"title_main_exact: Namja munje ŭi sidae\n"+
		"title_main_exact: Namja munje ŭi sidae\n"+
		"title_sms_compat_display: Namja munje ui sidae\n"+
		"title_2letter_s: na\n"+
		"title_1letter_s: n\n"+
		"title_vern_display: 남자 문제 의 시대\n"+
		"subtitle_vern_display: 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_t_cjk: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"fulltitle_vern_display: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_exact: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_exact: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_main_exact: 남자 문제 의 시대\n"+
		"title_main_exact: 남자 문제 의 시대\n"+
		"title_responsibility_display: 다가 후토시 지음 ; 책사소 옮김. / Taga Hut'osi chiŭm ; Ch'aeksaso omgim.\n"+
		"author_245c_t_cjk: 다가 후토시 지음 ; 책사소 옮김.\n"+
		"author_245c_t: Taga Hut'osi chiŭm ; Ch'aeksaso omgim.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testCorpAuthorWithN() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"110",'2',' ',"‡a Gerakan Pemuda Islam Indonesia. ‡b Mu'tamar ‡n (9th :"
				+ " ‡d 1959 : ‡c Jakarta, Indonesia)"));
		rec.dataFields.add(new DataField(3,"245",'1','0',"‡a Tjita dan daja pemuda Islam :"
				+ " ‡b menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d 31 Oktober 1959 di Djakarta."));
		String expected =
		"author_display: Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)\n"+
		"author_t: Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)\n"+
		"author_cts: Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)|"
		+ "Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)\n"+
		"author_facet: Gerakan Pemuda Islam Indonesia. Mu'tamar\n"+
		"author_corp_filing: gerakan pemuda islam indonesia mutamar\n"+
		"author_json: {\"name1\":\"Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)\","
		+ "\"search1\":\"Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia)\","
		+ "\"relator\":\"\",\"type\":\"Corporate Name\",\"authorizedForm\":false}\n"+
		"author_sort: gerakan pemuda islam indonesia mutamar 9th 1959 jakarta indonesia\n"+
		"title_sort: tjita dan daja pemuda islam menjongsong mutamar por gpii ke ix 25 sd 31 oktober 1959 "
		+ "di djakarta 48&\n"+
		"title_display: Tjita dan daja pemuda Islam\n"+
		"subtitle_display: menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d 31 Oktober 1959 di Djakarta\n"+
		"fulltitle_display: Tjita dan daja pemuda Islam : menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d"
		+ " 31 Oktober 1959 di Djakarta\n"+
		"title_t: Tjita dan daja pemuda Islam : menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d 31"
		+ " Oktober 1959 di Djakarta\n"+
		"title_t: Tjita dan daja pemuda Islam : menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d 31"
		+ " Oktober 1959 di Djakarta\n"+
		"title_exact: Tjita dan daja pemuda Islam : menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d"
		+ " 31 Oktober 1959 di Djakarta\n"+
		"title_exact: Tjita dan daja pemuda Islam : menjongsong Mu'tamar & P.O.R. G.P.I.I. ke IX 25 s/d"
		+ " 31 Oktober 1959 di Djakarta\n"+
		"title_main_exact: Tjita dan daja pemuda Islam\n"+
		"title_main_exact: Tjita dan daja pemuda Islam\n"+
		"title_sms_compat_display: Tjita dan daja pemuda Islam\n"+
		"title_2letter_s: tj\n"+
		"title_1letter_s: t\n"+
		"authortitle_facet: Gerakan Pemuda Islam Indonesia. Mu'tamar (9th : 1959 : Jakarta, Indonesia) |"
		+ " Tjita dan daja pemuda Islam\n"+
		"authortitle_filing: gerakan pemuda islam indonesia mutamar 9th 1959 jakarta indonesia 0000"
		+ " tjita dan daja pemuda islam\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testAuthorMislinkedToNonRomanTitle6507903()
			throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,1,"100",'1',' ',
				"‡6 880-01 ‡a Foucher, A. ‡q (Alfred), ‡d 1865-1952.",false));
		rec.dataFields.add(new DataField(2,"240",'1','0',
				"‡a Beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology. ‡l Chinese"));
		rec.dataFields.add(new DataField(3,1,"245",'0','0',
				"‡6 880-01 ‡a Fo jiao yi shu de zao qi jie duan = ‡b The beginnings of Buddhist art and other essays"
				+ " in Indian and Central-Asian archaeology / ‡c cA Fuxie (A. Foucher) zhu ; Wang Pingxian,"
				+ " Wei Wenjie yi ; Wang Jiqing shen jiao.",false));
		rec.dataFields.add(new DataField(4,1,"245",'0','0',
				"‡6 245-01/$1 ‡a 佛教艺术的早期阶段 = ‡b The beginnings of Buddhist art and other essays in Indian and"
				+ " Central-Asian archaeology / ‡c c阿・福歇 (A. Foucher) 著 ; 王平先, 魏文捷译 ; 王冀青审校.",true));
		String expected =
		"author_display: Foucher, A. (Alfred), 1865-1952.\n"+
		"author_t: Foucher, A. (Alfred), 1865-1952.\n"+
		"author_cts: Foucher, A. (Alfred), 1865-1952.|Foucher, A. (Alfred), 1865-1952.\n"+
		"author_facet: Foucher, A. (Alfred), 1865-1952\n"+
		"author_pers_filing: foucher a alfred 1865 1952\n"+
		"author_json: {\"name1\":\"Foucher, A. (Alfred), 1865-1952.\",\"search1\":\"Foucher, A. (Alfred), "
		+ "1865-1952.\",\"relator\":\"\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: Foucher, Alfred Charles Auguste, 1865-1952\n"+
		"author_sort: foucher a alfred 1865 1952\n"+
		"title_uniform_display: Beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology. "
		+ "Chinese|Beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology. "
		+ "Chinese|Foucher, A. (Alfred), 1865-1952.\n"+
		"authortitle_facet: Foucher, A. (Alfred), 1865-1952. | Beginnings of Buddhist art and other essays in "
		+ "Indian and Central-Asian archaeology. Chinese\n"+
		"authortitle_filing: foucher a alfred 1865 1952 0000 beginnings of buddhist art and other essays in indian "
		+ "and central asian archaeology chinese\n"+
		"title_uniform_t: Beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology. "
		+ "Chinese\n"+
		"title_uniform_t: Beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology. "
		+ "Chinese\n"+
		"title_sort: fo jiao yi shu de zao qi jie duan the beginnings of buddhist art and other essays in indian "
		+ "and central asian archaeology\n"+
		"title_display: Fo jiao yi shu de zao qi jie duan\n"+
		"subtitle_display: The beginnings of Buddhist art and other essays in Indian and Central-Asian archaeology\n"+
		"fulltitle_display: Fo jiao yi shu de zao qi jie duan = The beginnings of Buddhist art and other essays in "
		+ "Indian and Central-Asian archaeology\n"+
		"title_t: Fo jiao yi shu de zao qi jie duan = The beginnings of Buddhist art and other essays in Indian and "
		+ "Central-Asian archaeology\n"+
		"title_t: Fo jiao yi shu de zao qi jie duan = The beginnings of Buddhist art and other essays in Indian and "
		+ "Central-Asian archaeology\n"+
		"title_exact: Fo jiao yi shu de zao qi jie duan = The beginnings of Buddhist art and other essays in Indian "
		+ "and Central-Asian archaeology\n"+
		"title_exact: Fo jiao yi shu de zao qi jie duan = The beginnings of Buddhist art and other essays in Indian "
		+ "and Central-Asian archaeology\n"+
		"title_main_exact: Fo jiao yi shu de zao qi jie duan\n"+
		"title_main_exact: Fo jiao yi shu de zao qi jie duan\n"+
		"title_sms_compat_display: Fo jiao yi shu de zao qi jie duan\n"+
		"title_2letter_s: fo\n"+
		"title_1letter_s: f\n"+
		"title_vern_display: 佛教艺术的早期阶段\n"+
		"subtitle_vern_display: The beginnings of Buddhist art and other essays in Indian and Central-Asian "
		+ "archaeology\n"+
		"title_t_cjk: 佛教艺术的早期阶段 = The beginnings of Buddhist art and other essays in Indian and Central-Asian "
		+ "archaeology\n"+
		"fulltitle_vern_display: 佛教艺术的早期阶段 = The beginnings of Buddhist art and other essays in Indian and "
		+ "Central-Asian archaeology\n"+
		"title_exact: 佛教艺术的早期阶段 = The beginnings of Buddhist art and other essays in Indian and Central-Asian "
		+ "archaeology\n"+
		"title_exact: 佛教艺术的早期阶段 = The beginnings of Buddhist art and other essays in Indian and Central-Asian "
		+ "archaeology\n"+
		"title_main_exact: 佛教艺术的早期阶段\n"+
		"title_main_exact: 佛教艺术的早期阶段\n"+
		"authortitle_facet: Foucher, A. (Alfred), 1865-1952. | 佛教艺术的早期阶段\n"+
		"authortitle_filing: foucher a alfred 1865 1952 0000 佛教艺术的早期阶段\n"+
		"title_responsibility_display: c阿・福歇 (A. Foucher) 著 ; 王平先, 魏文捷译 ; 王冀青审校. / cA Fuxie (A. Foucher)"
		+ " zhu ; Wang Pingxian, Wei Wenjie yi ; Wang Jiqing shen jiao.\n"+
		"author_245c_t_cjk: c阿・福歇 (A. Foucher) 著 ; 王平先, 魏文捷译 ; 王冀青审校.\n"+
		"author_245c_t: cA Fuxie (A. Foucher) zhu ; Wang Pingxian, Wei Wenjie yi ; Wang Jiqing shen jiao.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testGoodLinksExceptRomanizedFieldsDontPointBackTo880Fields7940870()
			throws ClassNotFoundException, SQLException, IOException {
		// This is a poorly encoded example, which we will treat as "good enough" and produce no errors.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"100",'1',' ',"‡a Grebenshchikova, G. A., ‡e author."));
		rec.dataFields.add(new DataField(2,1,"100",'1','0',"‡6 100-01 ‡a Гребенщикова, Г. А, ‡e author.",true));
		rec.dataFields.add(new DataField(3,"245",'0','0',
				"‡a Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II"));
		rec.dataFields.add(new DataField(4,2,"245",'0','0',
				"‡6 245-02 ‡a Черноморский флот в период правления Екатерины II",true));
		String expected =
		"author_display: Гребенщикова, Г. А. / Grebenshchikova, G. A., author\n"+
		"author_cts: Гребенщикова, Г. А.|Гребенщикова, Г. А,|Grebenshchikova, G. A., author|Grebenshchikova, G. A.,\n"+
		"author_facet: Гребенщикова, Г. А.\n"+
		"author_facet: Grebenshchikova, G. A.\n"+
		"author_pers_filing: гребенщикова г а\n"+
		"author_pers_filing: grebenshchikova g a\n"+
		"author_t: Гребенщикова, Г. А, author\n"+
		"author_t: Grebenshchikova, G. A., author\n"+
		"author_json: {\"name1\":\"Гребенщикова, Г. А.\",\"search1\":\"Гребенщикова, Г. А,\","
		+ "\"name2\":\"Grebenshchikova, G. A., author\",\"search2\":\"Grebenshchikova, G. A.,\","
		+ "\"relator\":\"author\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: Grebenshchikova, Galina Aleksandrovna\n"+
		"author_sort: grebenshchikova g a\n"+
		"title_sort: chernomorskii flot v period pravleniia ekateriny ii\n"+
		"title_display: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"subtitle_display: \n"+
		"fulltitle_display: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"title_t: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"title_t: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"title_exact: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"title_exact: Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"title_sms_compat_display: Chernomorskii flot v period pravleniia Ekateriny II\n"+
		"title_2letter_s: ch\n"+
		"title_1letter_s: c\n"+
		"authortitle_facet: Grebenshchikova, G. A., | Chernomorskiĭ flot v period pravlenii︠a︡ Ekateriny II\n"+
		"authortitle_filing: grebenshchikova g a 0000 chernomorskii flot v period pravleniia ekateriny ii\n"+
		"title_vern_display: Черноморский флот в период правления Екатерины II\n"+
		"subtitle_vern_display: \n"+
		"title_t: Черноморский флот в период правления Екатерины II\n"+
		"title_t: Черноморский флот в период правления Екатерины II\n"+
		"fulltitle_vern_display: Черноморский флот в период правления Екатерины II\n"+
		"title_exact: Черноморский флот в период правления Екатерины II\n"+
		"title_exact: Черноморский флот в период правления Екатерины II\n"+
		"authortitle_facet: Гребенщикова, Г. А, | Черноморский флот в период правления Екатерины II\n"+
		"authortitle_filing: гребенщикова г а 0000 черноморскии флот в период правления екатерины ii\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testTwoDifferentAuthorFieldsWithDifferentTags6279795()
			throws ClassNotFoundException, SQLException, IOException {
		// This is a badly encoded example, and we expect a squawk from AuthorTitle.java about it.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "6279795";
		rec.dataFields.add(new DataField(1,0,"110",'1',' ',"‡a Korea (South). ‡b President (1993-1998 : Kim)",false));
		rec.dataFields.add(new DataField(2,0,"100",'1',' ',"‡6 100-00/$1 ‡a 金泳三, ‡d 1927-",true));
		String expected =
		"author_t: Korea (South). President (1993-1998 : Kim)\n"+
		"author_cts: Korea (South). President (1993-1998 : Kim)|Korea (South). President (1993-1998 : Kim)\n"+
		"author_facet: Korea (South). President (1993-1998 : Kim)\n"+
		"author_corp_filing: korea south president 1993 1998 kim\n"+
		"author_json: {\"name1\":\"Korea (South). President (1993-1998 : Kim)\","
		+ "\"search1\":\"Korea (South). President (1993-1998 : Kim)\",\"relator\":\"\","
		+ "\"type\":\"Corporate Name\",\"authorizedForm\":true}\n"+
		"author_t_cjk: 金泳三, 1927-\n"+
		"author_cts: 金泳三, 1927-|金泳三, 1927-\n"+
		"author_facet: 金泳三, 1927-\n"+
		"author_pers_filing: 金泳三 1927\n"+
		"author_json: {\"name1\":\"金泳三, 1927-\",\"search1\":\"金泳三, 1927-\","
		+ "\"relator\":\"\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
		"author_display: Korea (South). President (1993-1998 : Kim)\n"+
		"author_sort: korea south president 1993 1998 kim\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

	@Test
	public void testExtraneousMainAuthorVernacularEntry6197642()
			throws ClassNotFoundException, SQLException, IOException {
		// This is a badly encoded example, and we expect a squawk from AuthorTitle.java about it.
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "6197642";
		rec.dataFields.add(new DataField(1,1,"110",'2',' ',"‡6 880-01 ‡a Guo li gu gong bo wu yuan.",false));
		rec.dataFields.add(new DataField(2,0,"100",'1','0',"‡6 100-00/$1 ‡a 蔡玫芬.",true));
		rec.dataFields.add(new DataField(3,1,"110",'2','0',"‡6 110-01/$1 ‡a 國立故宮博物院.",true));
		String expected =
		"author_cts: 國立故宮博物院|國立故宮博物院.|Guo li gu gong bo wu yuan.|Guo li gu gong bo wu yuan.\n"+
		"author_facet: 國立故宮博物院\n"+
		"author_facet: Guo li gu gong bo wu yuan\n"+
		"author_corp_filing: 國立故宮博物院\n"+
		"author_corp_filing: guo li gu gong bo wu yuan\n"+
		"author_t_cjk: 國立故宮博物院.\n"+
		"author_t: Guo li gu gong bo wu yuan.\n"+
		"author_json: {\"name1\":\"國立故宮博物院\",\"search1\":\"國立故宮博物院.\",\"name2\":"
		+ "\"Guo li gu gong bo wu yuan.\",\"search2\":\"Guo li gu gong bo wu yuan.\","
		+ "\"relator\":\"\",\"type\":\"Corporate Name\",\"authorizedForm\":true}\n"+
		"authority_author_t: China (Republic : 1949- ). Chinese National Palace Museum\n"+
		"authority_author_t: China (Republic : 1949- ). Guo li gu gong bo wu yuan\n"+
		"authority_author_t: China (Republic : 1949- ). National Palace Museum\n"+
		"authority_author_t: China (Republic : 1949- ). 國立故宮博物院\n"+
		"authority_author_t_cjk: China (Republic : 1949- ). 國立故宮博物院\n"+
		"authority_author_t: Chinese National Palace Museum\n"+
		"authority_author_t: Chūka Minkoku Kokuritsu Kokyū Hakubutsuin\n"+
		"authority_author_t: Gu gong bo wu yuan (Taipei, Taiwan)\n"+
		"authority_author_t: Gu gong yuan (Taipei, Taiwan)\n"+
		"authority_author_t: Kokuritsu Kokyū Hakubutsuin\n"+
		"authority_author_t: Kuo li ku kung po wu yüan\n"+
		"authority_author_t: Musée national du Palais (Taipei, Taiwan)\n"+
		"authority_author_t: National Palace Museum (Taipei, Taiwan)\n"+
		"authority_author_t: Taibei gu gong bo wu yuan\n"+
		"authority_author_t: Taipei (Taiwan). Chinese National Palace Museum\n"+
		"authority_author_t: Taipei (Taiwan). Guo li gu gong bo wu yuan\n"+
		"authority_author_t: 台北 (台灣). 國立故宮博物院\n"+
		"authority_author_t_cjk: 台北 (台灣). 國立故宮博物院\n"+
		"authority_author_t: 台北故宮博物院\n"+
		"authority_author_t_cjk: 台北故宮博物院\n"+
		"authority_author_t: 國立故宮博物院\n"+
		"authority_author_t_cjk: 國立故宮博物院\n"+
		"authority_author_t: 故宮博物院 (Taipei, Taiwan)\n"+
		"authority_author_t_cjk: 故宮博物院 (Taipei, Taiwan)\n"+
		"author_t_cjk: 蔡玫芬.\n"+
		"author_cts: 蔡玫芬.|蔡玫芬.\n"+
		"author_facet: 蔡玫芬\n"+
		"author_pers_filing: 蔡玫芬\n"+
		"author_json: {\"name1\":\"蔡玫芬.\",\"search1\":\"蔡玫芬.\",\"relator\":\"\",\"type\":\"Personal Name\","
		+ "\"authorizedForm\":false}\n"+
		"author_display: 國立故宮博物院 / Guo li gu gong bo wu yuan.\n"+
		"author_sort: guo li gu gong bo wu yuan\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}

}
