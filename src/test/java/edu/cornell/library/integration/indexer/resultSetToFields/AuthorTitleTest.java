package edu.cornell.library.integration.indexer.resultSetToFields;

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
									"\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
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
		"title_responsibility_display: Mariano León Cupe, Jorge León Quispe.\n";
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
		"author_facet: Fewer, T. N\n"+
		"author_pers_filing: fewer t n\n"+
		"author_json: {\"name1\":\"Fewer, T. N.\",\"search1\":\"Fewer, T. N.\",\"type\":"+
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
		"title_sms_compat_display: Waterford people\n"+
		"title_2letter_s: wa\n"+
		"title_1letter_s: w\n"+
		"authortitle_facet: Fewer, T. N. | Waterford people\n"+
		"authortitle_filing: fewer t n 0000 waterford people\n"+
		"title_responsibility_display: T. N. Fewer.\n";
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
		"author_json: {\"name1\":\"Kalavrezos, Nicholas, speaker\",\"search1\":\"Kalavrezos, Nicholas,\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
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
		"title_responsibility_display: Nicholas Kalavrezos.\n";
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
		"author_json: {\"name1\":\"Speed, John, 1552?-1629, cartographer\",\"search1\":"+
							"\"Speed, John, 1552?-1629,\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
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
		"title_sms_compat_display: Britain's Tudor maps\n"+
		"title_2letter_s: br\n"+
		"title_1letter_s: b\n"+
		"title_responsibility_display: John Speed ; introduction by Nigel Nicolson ; country commentaries"+
									" by Alasdair Hawkyard.\n";
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
		"author_facet: Papadēmētropoulos, Loukas P\n"+
		"author_pers_filing: papademetropoulos loukas p\n"+
		"author_json: {\"name1\":\"Papadēmētropoulos, Loukas P., author\",\"search1\":"
				+ "\"Papadēmētropoulos, Loukas P.,\",\"type\":\"Personal Name\",\"authorizedForm\":true}\n"+
		"author_sort: papademetropoulos loukas p\n"+
		"title_sort: ennoia tou oikou ston euripide alkeste medeia hippolytos\n"+
		"title_display: Hē ennoia tou oikou ston Euripidē\n"+
		"subtitle_display: Alkēstē, Mēdeia, Hippolytos\n"+
		"fulltitle_display: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_t: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_t: ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_exact: Hē ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_exact: ennoia tou oikou ston Euripidē : Alkēstē, Mēdeia, Hippolytos\n"+
		"title_sms_compat_display: He ennoia tou oikou ston Euripide\n"+
		"title_2letter_s: en\n"+
		"title_1letter_s: e\n"+
		"authortitle_facet: Papadēmētropoulos, Loukas P., | ennoia tou oikou ston Euripidē\n"+
		"authortitle_filing: papademetropoulos loukas p 0000 ennoia tou oikou ston euripide\n"+
		"title_responsibility_display: Loukas Papadēmētropoulos.\n";
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
		"title_sms_compat_display: Aleksandr I, Mariia Pavlovna, Elizaveta Alekseevna\n"+
		"title_2letter_s: al\n"+
		"title_1letter_s: a\n"+
		"title_vern_display: Александр I, Мария Павловна, Елизавета Алексеевна\n"+
		"subtitle_vern_display: переписка из трех углов 1804-1826\n"+
		"title_t: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_t: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"fulltitle_vern_display: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_exact: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_exact: Александр I, Мария Павловна, Елизавета Алексеевна : переписка из трех углов 1804-1826\n"+
		"title_responsibility_display: подготовка писем Е. Дмитриевой и Ф. Шедеви. / podgotovka pisem E. Dmitrievoĭ i F. Shedevi.\n";
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
		+ "\"search2\":\"Taga, Futoshi, 1968-\",\"type\":\"Personal Name\",\"authorizedForm\":false}\n"+
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
		"title_sms_compat_display: Namja munje ui sidae\n"+
		"title_2letter_s: na\n"+
		"title_1letter_s: n\n"+
		"title_vern_display: 남자 문제 의 시대\n"+
		"subtitle_vern_display: 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_t_cjk: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"fulltitle_vern_display: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_exact: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_exact: 남자 문제 의 시대 = 男子問題の時代? : 젠더 와 교육 의 정치학\n"+
		"title_responsibility_display: 다가 후토시 지음 ; 책사소 옮김. / Taga Hut'osi chiŭm ; Ch'aeksaso omgim.\n";
		assertEquals( expected, AuthorTitle.generateSolrFields(rec, config).toString() );
	}
}
