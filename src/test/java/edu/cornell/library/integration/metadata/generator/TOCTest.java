package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class TOCTest extends DbBaseTest {
	SolrFieldGenerator gen = new TOC();

//	@BeforeClass
//	public static void setup() {
//		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
//		config = Config.loadConfig(requiredArgs);
//	}

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		setup("Current");
	}

	@Test
	public void testSimpleTOC() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"505",'0',' ',"‡a 12344567 (pbk.)"));
		String expected =
		"contents_display: 12344567 (pbk.)\n"+
		"toc_t: 12344567 (pbk.)\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testPartTOC() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"505",'2',' ',
				"‡g v. 2/2 (Sept. 1890), pp. 188-210. ‡t \"Great Krishna Mulvaney\" ‡r Rudyard Kipling."));
		String expected =
		"partial_contents_display: v. 2/2 (Sept. 1890), pp. 188-210. \"Great Krishna Mulvaney\" Rudyard Kipling.\n"+
		"title_addl_t: \"Great Krishna Mulvaney\"\n"+
		"author_addl_t: Rudyard Kipling.\n"+
		"toc_t: v. 2/2 (Sept. 1890), pp. 188-210. \"Great Krishna Mulvaney\" Rudyard Kipling.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testEnhancedAuthors() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"505",'0',' ',
				"‡g Vol. 2 / ‡r A cura di Gino Ruozzi. -- ‡g v. 3 / ‡r A cura di Carminella Biondi,"
				+ " Carla Pellandra, Elena Pessini."));
		String expected =
		"contents_display: Vol. 2 / A cura di Gino Ruozzi.\n"+
		"contents_display: v. 3 / A cura di Carminella Biondi, Carla Pellandra, Elena Pessini.\n"+
		"author_addl_t: A cura di Gino Ruozzi. --\n"+
		"author_addl_t: A cura di Carminella Biondi, Carla Pellandra, Elena Pessini.\n"+
		"toc_t: Vol. 2 / A cura di Gino Ruozzi. -- v. 3 / A cura di Carminella Biondi, Carla Pellandra, Elena Pessini.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testEnhancedTitles() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"505",'0','0',"‡g v. 1. ‡t Systematic handbook -- ‡g v. 2. ‡t Prayer."));
		String expected =
		"contents_display: v. 1. Systematic handbook\n"+
		"contents_display: v. 2. Prayer.\n"+
		"title_addl_t: Systematic handbook --\n"+
		"title_addl_t: Prayer.\n"+
		"toc_t: v. 1. Systematic handbook -- v. 2. Prayer.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNonRoman1() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,5,"505",'0','0',"‡6 880-05 ‡g \"A mozhet, i︠a︡ lishʹ pochva dli︠a︡ romana?\""
				+ " : ob avtore ėtikh vospominaniĭ / ‡r Irina Emelʹi︠a︡nova -- ‡t V plenu vremeni / ‡r Olʹga Ivinskai︠a︡ "
				+ "-- ‡t Legendy Potapovskogo pereulka / ‡r Irina Emelʹi︠a︡nova.", false));
		rec.dataFields.add(new DataField(2,5,"505",' ',' ',"‡6 505-05/(N ‡g \"А может, я лишь почва для романа?\""
				+ " : об авторе этих воспоминаний / ‡r Ирина Емельянова -- ‡t В плену времени / ‡r Ольга Ивинская"
				+ " -- ‡t Легенды Потаповского переулка / ‡r Ирина Емельянова.", true));
		String expected =
		"contents_display: \"А может, я лишь почва для романа?\" : об авторе этих воспоминаний / Ирина Емельянова\n"+
		"contents_display: В плену времени / Ольга Ивинская\n"+
		"contents_display: Легенды Потаповского переулка / Ирина Емельянова.\n"+
		"author_addl_t: Ирина Емельянова --\n"+
		"title_addl_t: В плену времени /\n"+
		"author_addl_t: Ольга Ивинская --\n"+
		"title_addl_t: Легенды Потаповского переулка /\n"+
		"author_addl_t: Ирина Емельянова.\n"+
		"toc_t: \"А может, я лишь почва для романа?\" : об авторе этих воспоминаний / Ирина Емельянова --"
		+ " В плену времени / Ольга Ивинская -- Легенды Потаповского переулка / Ирина Емельянова.\n"+
		"contents_display: \"A mozhet, i︠a︡ lishʹ pochva dli︠a︡ romana?\" : ob avtore ėtikh vospominaniĭ /"
		+ " Irina Emelʹi︠a︡nova\n"+
		"contents_display: V plenu vremeni / Olʹga Ivinskai︠a︡\n"+
		"contents_display: Legendy Potapovskogo pereulka / Irina Emelʹi︠a︡nova.\n"+
		"author_addl_t: Irina Emelʹi︠a︡nova --\n"+
		"title_addl_t: V plenu vremeni /\n"+
		"author_addl_t: Olʹga Ivinskai︠a︡ --\n"+
		"title_addl_t: Legendy Potapovskogo pereulka /\n"+
		"author_addl_t: Irina Emelʹi︠a︡nova.\n"+
		"toc_t: \"A mozhet, i︠a︡ lishʹ pochva dli︠a︡ romana?\" : ob avtore ėtikh vospominaniĭ / Irina Emelʹi︠a︡nova --"
		+ " V plenu vremeni / Olʹga Ivinskai︠a︡ -- Legendy Potapovskogo pereulka / Irina Emelʹi︠a︡nova.\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNonRomanCJK() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,7,"505",'0','0',"‡6 880-07 ‡g v. 1. ‡t Zhang quan : Nanjin zheng fu --"
				+ " ‡g v. 2. Fen qi : kang zhan ji zhan hou -- ‡g v. 3. Yi han : kang zhan ji zhan hou (xu).", false));
		rec.dataFields.add(new DataField(2,7,"505",' ',' ',"‡6 505-07/$1 ‡g v. 1. ‡t 掌權 : 南京政府 --"
				+ " ‡g v. 2. ‡t 奮起 : 抗戰及戰後 -- ‡g v. 3. ‡t 遺憾 : 抗戰及戰後(續).", true));
		String expected =
		"contents_display: v. 1. 掌權 : 南京政府\n"+
		"contents_display: v. 2. 奮起 : 抗戰及戰後\n"+
		"contents_display: v. 3. 遺憾 : 抗戰及戰後(續).\n"+
		"title_addl_t_cjk: 掌權 : 南京政府 --\n"+
		"title_addl_t_cjk: 奮起 : 抗戰及戰後 --\n"+
		"title_addl_t_cjk: 遺憾 : 抗戰及戰後(續).\n"+
		"toc_t_cjk: v. 1. 掌權 : 南京政府 -- v. 2. 奮起 : 抗戰及戰後 -- v. 3. 遺憾 : 抗戰及戰後(續).\n"+
		"contents_display: v. 1. Zhang quan : Nanjin zheng fu\n"+
		"contents_display: v. 2. Fen qi : kang zhan ji zhan hou\n"+
		"contents_display: v. 3. Yi han : kang zhan ji zhan hou (xu).\n"+
		"title_addl_t: Zhang quan : Nanjin zheng fu --\n"+
		"toc_t: v. 1. Zhang quan : Nanjin zheng fu -- v. 2. Fen qi : kang zhan ji zhan hou -- v. 3. Yi han :"
		+ " kang zhan ji zhan hou (xu).\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNonRomanMultiplePairs() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,7,"505",'0','0',"‡6 880-07 ‡a v.1 -- v.2", false));
		rec.dataFields.add(new DataField(2,8,"505",'0','0',"‡6 880-08 ‡a v.3 -- v.4", false));
		rec.dataFields.add(new DataField(3,7,"505",' ',' ',"‡6 505-07/$1 ‡a v.1 CJK -- v.2 CJK", true));
		rec.dataFields.add(new DataField(4,8,"505",' ',' ',"‡6 505-08/$1 ‡a v.3 CJK -- v.4 CJK", true));
		String expected =
		"contents_display: v.1 CJK\n"+
		"contents_display: v.2 CJK\n"+
		"toc_t_cjk: v.1 CJK -- v.2 CJK\n"+
		"contents_display: v.1\n"+
		"contents_display: v.2\n"+
		"toc_t: v.1 -- v.2\n"+
		"contents_display: v.3 CJK\n"+
		"contents_display: v.4 CJK\n"+
		"toc_t_cjk: v.3 CJK -- v.4 CJK\n"+
		"contents_display: v.3\n"+
		"contents_display: v.4\n"+
		"toc_t: v.3 -- v.4\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void syndeticsTocIndex() throws ClassNotFoundException, SQLException, IOException {
	
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.id = "13091615";
		rec.dataFields.add(new DataField(1,"020",' ',' ',"‡a 0691202184 ‡q (hardcover)"));
		String expected_prefix =
		"title_addl_t: Map: Xinjiang Uyghur Autonomous Region\n" + 
		"toc_t: Map: Xinjiang Uyghur Autonomous Region\n" + 
		"title_addl_t: Foreword\n" + 
		"author_addl_t: Ben Emmerson\n" + 
		"toc_t: Foreword Ben Emmerson\n" + 
		"title_addl_t: Preface\n" + 
		"toc_t: Preface\n" + 
		"title_addl_t: Introduction\n" + 
		"toc_t: Introduction\n" + 
		"title_addl_t: Colonialism, 1759-2001\n" + 
		"toc_t: Colonialism, 1759-2001\n" + 
		"title_addl_t: How the Uyghurs became a 'terrorist threat'\n" + 
		"toc_t: How the Uyghurs became a 'terrorist threat'";
		assertTrue( this.gen.generateSolrFields(rec, config).toString().startsWith(expected_prefix) );
	}
}

