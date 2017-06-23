package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

@SuppressWarnings("static-method")
public class TitleSeriesTest {

	@Test
	public void testField830() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"830",' ','0',"‡a International conciliation (Monthly) ; ‡v no. 164"));
		String expected =
		"title_series_display: International conciliation (Monthly) ; no. 164\n"+
		"title_series_cts: International conciliation (Monthly) ; no. 164|International conciliation (Monthly) ;\n"+
		"title_series_t: International conciliation (Monthly) ; no. 164\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField830WithArticle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"830",' ','3',"‡a La Mémoire du siècle ; ‡v 29"));
		String expected =
		"title_series_display: La Mémoire du siècle ; 29\n"+
		"title_series_cts: La Mémoire du siècle ; 29|La Mémoire du siècle ;\n"+
		"title_series_t: La Mémoire du siècle ; 29\n"+
		"title_series_t: Mémoire du siècle ; 29\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField800() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"800",'0',' ',
				"‡a Josquin, ‡c des Prez, ‡d -1521. ‡t Works. ‡f 1987. ‡k Critical commentary ; ‡v v. 20."));
		String expected =
		"title_series_display: Josquin, des Prez, -1521. | Works. 1987. Critical commentary ; v. 20.\n"+
		"title_series_cts: Josquin, des Prez, -1521. Works. 1987. Critical commentary ; v. 20.|Works. Critical commentary ;|Josquin, des Prez, -1521.\n"+
		"authortitle_filing: josquin des prez 1521 0000 works critical commentary\n"+
		"authortitle_facet: Josquin, des Prez, -1521. | Works. Critical commentary\n"+
		"title_series_t: Works. 1987. Critical commentary ; v. 20.\n"+
		"title_series_t: Works. Critical commentary ;\n"+
		"author_addl_t: Josquin, des Prez, -1521.\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField800_Error_NoTitle() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"800",'1',' ',
				"‡a Jazāʾirī, Abū Bakr Jābir. ‡b Min rasāʾil al-daʻwah."));
		String expected =
		"title_series_display: Jazāʾirī, Abū Bakr Jābir. Min rasāʾil al-daʻwah.\n"+
		"title_series_cts: Jazāʾirī, Abū Bakr Jābir. Min rasāʾil al-daʻwah.|Jazāʾirī, Abū Bakr Jābir. Min rasāʾil al-daʻwah.\n"+
		"title_series_t: Jazāʾirī, Abū Bakr Jābir. Min rasāʾil al-daʻwah.\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField810_CJK() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,6,"810",'2',' ',"‡6 880-06 ‡a Beijing da xue. ‡b Yan jiu suo."
				+ " ‡b Guo xue men. ‡t Guo li Beijing da xue yan jiu suo guo xue men cong shu.",false));
		rec.dataFields.add(new DataField(2,6,"810",'2',' ',
				"‡6 810-06/$1 ‡a 北京大學. ‡b 硏究所. ‡b 國學門. ‡t 國立北京大學研究所國學門叢書.",true));
		String expected =
		"title_series_display: 北京大學. 硏究所. 國學門. | 國立北京大學研究所國學門叢書.\n"+
		"title_series_cts: 北京大學. 硏究所. 國學門. 國立北京大學研究所國學門叢書.|國立北京大學研究所國學門叢書."
		+ "|北京大學. 硏究所. 國學門.\n"+
		"authortitle_filing: 北京大學 硏究所 國學門 0000 國立北京大學研究所國學門叢書\n"+
		"authortitle_facet: 北京大學. 硏究所. 國學門. | 國立北京大學研究所國學門叢書\n"+
		"title_series_t_cjk: 國立北京大學研究所國學門叢書.\n"+
		"author_addl_t_cjk: 北京大學. 硏究所. 國學門.\n"+
		"title_series_display: Beijing da xue. Yan jiu suo. Guo xue men. "
		+ "| Guo li Beijing da xue yan jiu suo guo xue men cong shu.\n"+
		"title_series_cts: Beijing da xue. Yan jiu suo. Guo xue men. Guo li Beijing da xue yan jiu suo guo xue"
		+ " men cong shu.|Guo li Beijing da xue yan jiu suo guo xue men cong shu.|Beijing da xue. Yan jiu suo."
		+ " Guo xue men.\n"+
		"authortitle_filing: beijing da xue yan jiu suo guo xue men "
		+ "0000 guo li beijing da xue yan jiu suo guo xue men cong shu\n"+
		"authortitle_facet: Beijing da xue. Yan jiu suo. Guo xue men. "
		+ "| Guo li Beijing da xue yan jiu suo guo xue men cong shu\n"+
		"title_series_t: Guo li Beijing da xue yan jiu suo guo xue men cong shu.\n"+
		"author_addl_t: Beijing da xue. Yan jiu suo. Guo xue men.\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField811_with_OCLC_ID() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"811",'2',' ',"‡a United Nations Issues Conference."
				+ " ‡t Report of the ... United Nations Issues Conference ‡x 0743-9180 ; ‡v 30th."));
		String expected =
		"title_series_display: United Nations Issues Conference. | Report of the ... United Nations Issues Conference 30th.\n"+
		"title_series_cts: United Nations Issues Conference. Report of the ... United Nations Issues Conference 30th.|Report of the ... United Nations Issues Conference|United Nations Issues Conference.\n"+
		"authortitle_filing: united nations issues conference 0000 report of the united nations issues conference\n"+
		"authortitle_facet: United Nations Issues Conference. | Report of the ... United Nations Issues Conference\n"+
		"title_series_t: Report of the ... United Nations Issues Conference 30th.\n"+
		"author_addl_t: United Nations Issues Conference.\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField440() {
		// Field 440 is obsolete, but exists more than 660k Cornell catalog records
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"440",' ','0',"‡a Medieval and early modern sources online"));
		rec.dataFields.add(new DataField(2,"440",' ','0',
				"‡a [Reports of the Royal Commission on Historical Manuscripts ; ‡v 11.5]"));
		String expected =
		"title_series_display: Medieval and early modern sources online\n"+
		"title_series_cts: Medieval and early modern sources online|Medieval and early modern sources online\n"+
		"title_series_t: Medieval and early modern sources online\n"+
		"title_series_display: [Reports of the Royal Commission on Historical Manuscripts ; 11.5]\n"+
		"title_series_cts: [Reports of the Royal Commission on Historical Manuscripts ; 11.5]|[Reports of the Royal Commission on Historical Manuscripts ;\n"+
		"title_series_t: [Reports of the Royal Commission on Historical Manuscripts ; 11.5]\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField440_Article() {
		// Field 440 is obsolete, but exists more than 660k Cornell catalog records
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"440",' ','5',
				"‡a [The Winthrop Pickard Bell lectures in Maritime studies] ; ‡v 1982-1983"));
		String expected =
		"title_series_display: [The Winthrop Pickard Bell lectures in Maritime studies] ; 1982-1983\n"+
		"title_series_cts: [The Winthrop Pickard Bell lectures in Maritime studies] ; 1982-1983|[The Winthrop Pickard Bell lectures in Maritime studies] ;\n"+
		"title_series_t: [The Winthrop Pickard Bell lectures in Maritime studies] ; 1982-1983\n"+
		"title_series_t: [Winthrop Pickard Bell lectures in Maritime studies] ; 1982-1983\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField490() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"490",'1',' ',
				"‡a Pleadings, oral arguments, documents = ‡a Mémoires, plaidoiries et documents"));
		String expected =
		"title_series_display: Pleadings, oral arguments, documents = Mémoires, plaidoiries et documents\n"+
		"title_series_cts: Pleadings, oral arguments, documents = Mémoires, plaidoiries et documents|Pleadings, oral arguments, documents = Mémoires, plaidoiries et documents\n"+
		"title_series_t: Pleadings, oral arguments, documents = Mémoires, plaidoiries et documents\n";
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testField490_830() {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"490",'1',' ',
				"‡a Pleadings, oral arguments, documents = ‡a Mémoires, plaidoiries et documents"));
		rec.dataFields.add(new DataField(2,"830",' ','0',"‡a Pleadings, oral arguments, documents."));
		String expected =
		"title_series_t: Pleadings, oral arguments, documents = Mémoires, plaidoiries et documents\n"+
		"title_series_display: Pleadings, oral arguments, documents.\n"+
		"title_series_cts: Pleadings, oral arguments, documents.|Pleadings, oral arguments, documents.\n"+
		"title_series_t: Pleadings, oral arguments, documents.\n";
//		System.out.println( TitleSeries.generateSolrFields(rec, null).toString().replaceAll("\"","\\\\\""));
		assertEquals( expected, TitleSeries.generateSolrFields(rec, null).toString() );
	}
}
