package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;

@SuppressWarnings("static-method")
public class PubInfoTest {

	@Test
	public void test9586284() {
		ControlField f1 = new ControlField(1,"008","160630s2014    oruab   ob   f000 0 eng c");
		DataField f2 = new DataField(3,"264");
		f2.ind2 = '1';
		f2.subfields.add(new Subfield(1, 'a', "Eugene, Ore. :"));
		f2.subfields.add(new Subfield(2, 'b', "University of Oregon,"));
		f2.subfields.add(new Subfield(3, 'c', "2014."));
		DataField f3 = new DataField(4,"264");
		f3.ind2 = '2';
		f3.subfields.add(new Subfield(1, 'a', "Omaha, Neb. :"));
		f3.subfields.add(new Subfield(2, 'b', "National Park Service"));
		MarcRecord rec = new MarcRecord();
		rec.controlFields.add(f1);
		rec.dataFields.add(f2);
		rec.dataFields.add(f3);
		List<SolrField> sfs = PubInfo.generateSolrFields(rec).fields;
		assertEquals( 15, sfs.size() );
		assertEquals("pub_date_sort",         sfs.get(0).fieldName);
		assertEquals("2014",                  sfs.get(0).fieldValue);
		assertEquals("pub_date_facet",        sfs.get(1).fieldName);
		assertEquals("2014",                  sfs.get(1).fieldValue);
		assertEquals("pub_info_display",      sfs.get(2).fieldName);
		assertEquals("Eugene, Ore. : University of Oregon, 2014.",
				                              sfs.get(2).fieldValue);
		assertEquals("pubplace_display",      sfs.get(3).fieldName);
		assertEquals("Eugene, Ore.",          sfs.get(3).fieldValue);
		assertEquals("pubplace_t",            sfs.get(4).fieldName);
		assertEquals("Eugene, Ore.",          sfs.get(4).fieldValue);
		assertEquals("publisher_display",     sfs.get(5).fieldName);
		assertEquals("University of Oregon",  sfs.get(5).fieldValue);
		assertEquals("publisher_t",           sfs.get(6).fieldName);
		assertEquals("University of Oregon",  sfs.get(6).fieldValue);
		assertEquals("pub_dist_display",      sfs.get(7).fieldName);
		assertEquals("Omaha, Neb. : National Park Service",
				                              sfs.get(7).fieldValue);
		assertEquals("pubplace_display",      sfs.get(8).fieldName);
		assertEquals("Omaha, Neb.",           sfs.get(8).fieldValue);
		assertEquals("pubplace_t",            sfs.get(9).fieldName);
		assertEquals("Omaha, Neb.",           sfs.get(9).fieldValue);
		assertEquals("publisher_display",     sfs.get(10).fieldName);
		assertEquals("National Park Service", sfs.get(10).fieldValue);
		assertEquals("publisher_t",           sfs.get(11).fieldName);
		assertEquals("National Park Service", sfs.get(11).fieldValue);
		assertEquals("pub_date_display",      sfs.get(12).fieldName);
		assertEquals("2014",                  sfs.get(12).fieldValue);
		assertEquals("pub_date_t",            sfs.get(13).fieldName);
		assertEquals("    ",                  sfs.get(13).fieldValue);
		assertEquals("pub_date_t",            sfs.get(14).fieldName);
		assertEquals("2014",                  sfs.get(14).fieldValue);
	}

//008 160630s2014    oruab   ob   f000 0 eng c
//264  1 ‡a Eugene, Ore. : ‡b University of Oregon, ‡c 2014.
//264  2 ‡a Omaha, Neb. : ‡b National Park Service

	@Test
	public void test8631644() {
		ControlField f1 = new ControlField(1,"008","140902p20142013njuuunn           n zxx d");
		DataField f2 = new DataField(3,"264");
		f2.ind2 = '1';
		f2.subfields.add(new Subfield(1, 'a', "[Jersey City, New Jersey] :"));
		f2.subfields.add(new Subfield(2, 'b', "Erstwhile Records,"));
		f2.subfields.add(new Subfield(3, 'c', "[2014]"));
		DataField f3 = new DataField(4,"264");
		f3.ind2 = '4';
		f3.subfields.add(new Subfield(1, 'c', "℗2014"));
		f3.subfields.add(new Subfield(2, 'c', "©2014"));
		MarcRecord rec = new MarcRecord();
		rec.controlFields.add(f1);
		rec.dataFields.add(f2);
		rec.dataFields.add(f3);
		List<SolrField> sfs = PubInfo.generateSolrFields(rec).fields;
		assertEquals( 14, sfs.size() );
		assertEquals("pub_date_sort",            sfs.get(0).fieldName);
		assertEquals("2013",                     sfs.get(0).fieldValue);
		assertEquals("pub_date_facet",           sfs.get(1).fieldName);
		assertEquals("2013",                     sfs.get(1).fieldValue);
		assertEquals("pub_info_display",         sfs.get(2).fieldName);
		assertEquals("[Jersey City, New Jersey] : Erstwhile Records, [2014]",
                                                 sfs.get(2).fieldValue);
		assertEquals("pubplace_display",         sfs.get(3).fieldName);
		assertEquals("[Jersey City, New Jersey]",sfs.get(3).fieldValue);
		assertEquals("pubplace_t",               sfs.get(4).fieldName);
		assertEquals("[Jersey City, New Jersey]",sfs.get(4).fieldValue);
		assertEquals("publisher_display",        sfs.get(5).fieldName);
		assertEquals("Erstwhile Records",        sfs.get(5).fieldValue);
		assertEquals("publisher_t",              sfs.get(6).fieldName);
		assertEquals("Erstwhile Records",        sfs.get(6).fieldValue);
		assertEquals("pub_copy_display",         sfs.get(7).fieldName);
		assertEquals("℗2014 ©2014",              sfs.get(7).fieldValue);
		assertEquals("pub_date_display",         sfs.get(8).fieldName);
		assertEquals("2014",                     sfs.get(8).fieldValue);
		assertEquals("pub_date_t",               sfs.get(9).fieldName);
		assertEquals("2013",                     sfs.get(9).fieldValue);
		assertEquals("pub_date_t",               sfs.get(10).fieldName);
		assertEquals("2014",                     sfs.get(10).fieldValue);
		assertEquals("pub_date_t",               sfs.get(11).fieldName);
		assertEquals("[2014]",                   sfs.get(11).fieldValue);
		assertEquals("pub_date_t",               sfs.get(12).fieldName);
		assertEquals("©2014",                    sfs.get(12).fieldValue);
		assertEquals("pub_date_t",               sfs.get(13).fieldName);
		assertEquals("℗2014",                    sfs.get(13).fieldValue);
	}
//008 140902p20142013njuuunn           n zxx d
//264  1 ‡a [Jersey City, New Jersey] : ‡b Erstwhile Records, ‡c [2014]
//264  4 ‡c ℗2014, ‡c ©2014

	@Test
	public void test5073103Without008OrMain260() {
		DataField f2 = new DataField(3,3,"260",true);
		f2.ind2 = '1';
		f2.subfields.add(new Subfield(1, '6', "260-03/$1"));
		f2.subfields.add(new Subfield(2, 'a', "東京 :"));
		f2.subfields.add(new Subfield(3, 'b', "吉川弘文館,"));
		f2.subfields.add(new Subfield(4, 'c', "2004."));
		MarcRecord rec = new MarcRecord();
		rec.dataFields.add(f2);
		List<SolrField> sfs = PubInfo.generateSolrFields(rec).fields;
		assertEquals( 9, sfs.size() );
		assertEquals("pub_info_display",         sfs.get(0).fieldName);
		assertEquals("東京 : 吉川弘文館, 2004.",    sfs.get(0).fieldValue);
		assertEquals("pubplace_t_cjk",           sfs.get(1).fieldName);
		assertEquals("東京 :",                    sfs.get(1).fieldValue);
		assertEquals("publisher_t_cjk",          sfs.get(2).fieldName);
		assertEquals("吉川弘文館,",                sfs.get(2).fieldValue);
		assertEquals("pubplace_display",          sfs.get(3).fieldName);
		assertEquals("東京",                      sfs.get(3).fieldValue);
		assertEquals("pubplace_t",               sfs.get(4).fieldName);
		assertEquals("東京",                      sfs.get(4).fieldValue);
		assertEquals("publisher_display",        sfs.get(5).fieldName);
		assertEquals("吉川弘文館",                 sfs.get(5).fieldValue);
		assertEquals("publisher_t",              sfs.get(6).fieldName);
		assertEquals("吉川弘文館",                 sfs.get(6).fieldValue);
		assertEquals("pub_date_display",          sfs.get(7).fieldName);
		assertEquals("2004",                      sfs.get(7).fieldValue);
		assertEquals("pub_date_t",                sfs.get(8).fieldName);
		assertEquals("2004",                      sfs.get(8).fieldValue);
	}
// 880 ‡6 260-03/$1 ‡a 東京 : ‡b 吉川弘文館, ‡c 2004.
}
