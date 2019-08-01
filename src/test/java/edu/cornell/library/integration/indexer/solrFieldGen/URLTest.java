package edu.cornell.library.integration.indexer.solrFieldGen;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class URLTest {

	SolrFieldGenerator gen = new URL();

	@Test   //8637892 DISCOVERYACCESS-2947
	public void testMultipleAccessWithDifferentTOU() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡3 Full text available from Ebrary The Arts Subscription Collection ‡i ssid=ssj0000907852;"
				+ " dbcode=AAGPP; providercode=PRVAHD ‡u http://proxy.library.cornell.edu/login?"
				+ "url=http://site.ebrary.com/lib/cornell/Top?id=10657875 ‡z Connect to text."));
		rec.dataFields.add(new DataField(2,"856",'4','0',
				"‡3 Full text available from Safari Technical Books ‡i ssid=ssj0000907852; dbcode=DRU;"
				+ " providercode=PRVPQU ‡u http://proxy.library.cornell.edu/login?"
				+ "url=http://proquest.safaribooksonline.com/9781118529669 ‡z Connect to text."));
		MarcRecord holdings = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdings.id = "1";
		holdings.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		rec.holdings.add(holdings);
		String expected =
		"url_access_display: http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/"
		+ "cornell/Top?id=10657875|Full text available from Ebrary The Arts Subscription Collection"
		+ " Connect to text.\n"+
		"notes_t: Full text available from Ebrary The Arts Subscription Collection Connect to text.\n"+
		"url_access_json: {\"providercode\":\"PRVAHD\",\"dbcode\":\"AAGPP\",\"description\":"
		+ "\"Full text available from Ebrary The Arts Subscription Collection Connect to text.\","
		+ "\"ssid\":\"ssj0000907852\",\"url\":\"http://proxy.library.cornell.edu/login?url="
		+ "http://site.ebrary.com/lib/cornell/Top?id=10657875\"}\n"+
		"url_access_display: http://proxy.library.cornell.edu/login?url=http://proquest."
		+ "safaribooksonline.com/9781118529669|Full text available from Safari Technical Books "
		+ "Connect to text.\n"+
		"notes_t: Full text available from Safari Technical Books Connect to text.\n"+
		"url_access_json: {\"providercode\":\"PRVPQU\",\"dbcode\":\"DRU\",\"description\":"
		+ "\"Full text available from Safari Technical Books Connect to text.\",\"ssid\":"
		+ "\"ssj0000907852\",\"url\":\"http://proxy.library.cornell.edu/login?url="
		+ "http://proquest.safaribooksonline.com/9781118529669\"}\n"+
		"online: Online\n";
		assertEquals( expected, gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNoTOU() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡3 Full text available from Ebrary The Arts Subscription Collection ‡u"
				+ " http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?"
				+ "id=10657875 ‡z Connect to text."));
		MarcRecord holdings = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdings.id = "1";
		holdings.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		rec.holdings.add(holdings);
		String expected =
		"url_access_display: http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?"
		+ "id=10657875|Full text available from Ebrary The Arts Subscription Collection Connect to text.\n"+
		"notes_t: Full text available from Ebrary The Arts Subscription Collection Connect to text.\n"+
		"url_access_json: {\"description\":\"Full text available from Ebrary The Arts Subscription Collection"
		+ " Connect to text.\",\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/"
		+ "cornell/Top?id=10657875\"}\n"+
		"online: Online\n";
		assertEquals( expected, gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testFindingAidURL() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','2',"‡3 Finding aid ‡u http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107"));
		String expected =
		"url_findingaid_display: http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107|Finding aid\n" + 
		"notes_t: Finding aid\n";
		assertEquals( expected, gen.generateSolrFields(rec, null).toString() );
	}
	
	@Test
	public void testJustURL() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		MarcRecord holdings = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdings.id = "1";
		holdings.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		rec.holdings.add(holdings);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡u http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		String expected =
		"url_access_display: http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top"
		+ "?id=10657875\n"+
		"url_access_json: {\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top"
		+ "?id=10657875\"}\n"+
		"online: Online\n";
		assertEquals( expected, gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testBookplateURL() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "9489596";
		holdingRec.dataFields.add(new DataField(1,"856",'4',' ',
				"‡u http://plates.library.cornell.edu/donor/DNR00450 ‡z From the Estate of Charles A. Leslie."));
		bibRec.holdings.add(holdingRec);
		String expected =
		"donor_t: From the Estate of Charles A. Leslie.\n"+
		"donor_s: DNR00450\n"+
		"url_bookplate_display: http://plates.library.cornell.edu/donor/DNR00450|From the Estate of Charles A. Leslie.\n"+
		"notes_t: From the Estate of Charles A. Leslie.\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testAccessLinkThatMatchesOtherLinkPattern() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "10205060";
		bibRec.dataFields.add(new DataField(1,"856",'4',' ',
		"‡i dbcode=JTX; providercode=PRVAWR "+
		"‡u http://proxy.library.cornell.edu/login?url=https://www.taylorfrancis.com/books/e/9781466552609 "+
		"‡z Full text is available via download of individual chapter PDFs; scroll down for full table of contents."));
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"url_access_display: http://proxy.library.cornell.edu/login?url=https://www.taylorfrancis.com/books/e/"
		+  "9781466552609|Full text is available via download of individual chapter PDFs; scroll down for full"
		+  " table of contents.\n" + 
		"notes_t: Full text is available via download of individual chapter PDFs;"
		      + " scroll down for full table of contents.\n" + 
		"url_access_json: {\"providercode\":\"PRVAWR\",\"dbcode\":\"JTX\","
		                + "\"description\":\"Full text is available via download of individual chapter PDFs;"
		                                 + " scroll down for full table of contents.\","
		+ "\"url\":\"http://proxy.library.cornell.edu/login?url=https://www.taylorfrancis.com/books/e/9781466552609\"}\n" + 
		"online: Online\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void otherContentLinkPrintBook() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "9142371";
		bibRec.dataFields.add(new DataField(1,"856",'7',' ',
		"‡3 View cover art "+
		"‡u http://midwesttapes.com/images/movies/000/000/000/011/353/000000000011353737.jpg ‡2 http"));
//		System.out.println(gen.generateSolrFields(bibRec, null).toString());
		String expected =
		"url_other_display: http://midwesttapes.com/images/movies/000/000/000/011/353/000000000011353737.jpg|View cover art\n" + 
		"notes_t: View cover art\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void otherContentLinkOnlineBook() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "5962354";
		bibRec.dataFields.add(new DataField(1,"856",'4','1',
		"‡3 Available from the U.S. Government Printing Office. Table of contents only "+
		"‡i dbcode=ACAJP; providercode=PRVLSH "+
		"‡u http://www.loc.gov/catdir/enhancements/fy0667/2006046631-t.html\n"));
		bibRec.dataFields.add(new DataField(2,"856",'4','1',
		"‡3 Available from the U.S. Government Printing Office. "+
		"‡i dbcode=ACAJP; providercode=PRVLSH "+
		"‡u http://purl.access.gpo.gov/GPO/LPS77292\n"));
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "1";
		holdingRec.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
		bibRec.holdings.add(holdingRec);
		String expected =
		"url_other_display: http://www.loc.gov/catdir/enhancements/fy0667/2006046631-t.html|"
		+ "Available from the U.S. Government Printing Office. Table of contents only\n" + 
		"notes_t: Available from the U.S. Government Printing Office. Table of contents only\n" + 
		"url_access_display: http://purl.access.gpo.gov/GPO/LPS77292|Available from the U.S. Government"
		+ " Printing Office.\n" + 
		"notes_t: Available from the U.S. Government Printing Office.\n" + 
		"url_access_json: {\"providercode\":\"PRVLSH\",\"dbcode\":\"ACAJP\",\"description\":"
		+ "\"Available from the U.S. Government Printing Office.\","
		+ "\"url\":\"http://purl.access.gpo.gov/GPO/LPS77292\"}\n" + 
		"online: Online\n";
		assertEquals( expected, gen.generateSolrFields(bibRec, null).toString() );
	}

}
