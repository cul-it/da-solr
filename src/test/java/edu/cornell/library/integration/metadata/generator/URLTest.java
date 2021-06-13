package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

public class URLTest {

	SolrFieldGenerator gen = new URL();
	static MarcRecord online ;

	@BeforeClass
	public static void createServRemoHolding() {
		online = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		online.id = "1";
		online.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
	}

	@Test
	public void ebscoTitleLink() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','0',
		"‡i 2471499 "+
		"‡y Click here to find online versions of this title. "+
		"‡u https://search.ebscohost.com/login.aspx?CustID=s9001366&db=edspub&type=44&"
		+ "bQuery=AN%202471499&direct=true&site=pfi-live"));
		rec.marcHoldings.add(online);
		String expected =
		"ebsco_title_facet: 2471499\n" + 
		"notes_t: Click here to find online versions of this title.\n" + 
		"url_access_json: {"
		+ "\"titleid\":\"2471499\","
		+ "\"description\":\"Click here to find online versions of this title.\","
		+ "\"url\":\"https://search.ebscohost.com/login.aspx?CustID=s9001366&db=edspub&type=44&"
		+           "bQuery=AN%202471499&direct=true&site=pfi-live\"}\n" + 
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void userLimitsIn899() throws ClassNotFoundException, SQLException, IOException {
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.id = "9298323";
			rec.dataFields.add(new DataField(1,"856",'4','0',
					"‡u http://proxy.library.cornell.edu/login"
					+ "?url=http://site.ebrary.com/lib/cornell/docDetail.action?docID=11113926 "+
					"‡z Connect to full text"));
			rec.dataFields.add(new DataField(2,"899",' ',' ',"‡a ebraryebks3u"));
			rec.marcHoldings.add(online);
			String expected = 
			"notes_t: Connect to full text\n" + 
			"url_access_json: {"
			+ "\"description\":\"Connect to full text\","
			+ "\"url\":\"http://proxy.library.cornell.edu/login?url="
			+           "http://site.ebrary.com/lib/cornell/docDetail.action?docID=11113926\","
			+ "\"users\":3}\n" + 
			"online: Online\n";
			assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
		}
		{
			MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
			rec.id = "10821629";
			rec.dataFields.add(new DataField(1,"856",'4','0',
					"‡3 Available from Brepols "+
					"‡i dbcode=~9u; providercode=PRVBRP "+
					"‡u http://proxy.library.cornell.edu/login?"
					+     "url=http://www.brepolsonline.net/doi/book/10.1484/M.CURSOR-EB.6.09070802050003050302020604 "+
					"‡z Connect to full text."));
			rec.dataFields.add(new DataField(2,"899",'2',' ',"‡a PRVBRP_~9u"));
			rec.marcHoldings.add(online);
			String expected =
			"notes_t: Available from Brepols Connect to full text.\n"+
			"url_access_json: {"
			+  "\"providercode\":\"PRVBRP\",\"dbcode\":\"~9u\","
			+  "\"description\":\"Available from Brepols Connect to full text.\","
			+  "\"url\":\"http://proxy.library.cornell.edu/login?"
			+            "url=http://www.brepolsonline.net/doi/book/10.1484/M.CURSOR-EB.6.09070802050003050302020604\","
			+  "\"users\":9}\n"+
			"online: Online\n";
			assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
		}
	}

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
		rec.marcHoldings.add(online);
		String expected =
		"notes_t: Full text available from Ebrary The Arts Subscription Collection Connect to text.\n"+
		"url_access_json: {\"providercode\":\"PRVAHD\",\"dbcode\":\"AAGPP\",\"description\":"
		+ "\"Full text available from Ebrary The Arts Subscription Collection Connect to text.\","
		+ "\"ssid\":\"ssj0000907852\",\"url\":\"http://proxy.library.cornell.edu/login?url="
		+ "http://site.ebrary.com/lib/cornell/Top?id=10657875\"}\n"+
		"notes_t: Full text available from Safari Technical Books Connect to text.\n"+
		"url_access_json: {\"providercode\":\"PRVPQU\",\"dbcode\":\"DRU\",\"description\":"
		+ "\"Full text available from Safari Technical Books Connect to text.\",\"ssid\":"
		+ "\"ssj0000907852\",\"url\":\"http://proxy.library.cornell.edu/login?url="
		+ "http://proquest.safaribooksonline.com/9781118529669\"}\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNoTOU() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡3 Full text available from Ebrary The Arts Subscription Collection ‡u"
				+ " http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?"
				+ "id=10657875 ‡z Connect to text."));
		rec.marcHoldings.add(online);
		String expected =
		"notes_t: Full text available from Ebrary The Arts Subscription Collection Connect to text.\n"+
		"url_access_json: {\"description\":\"Full text available from Ebrary The Arts Subscription Collection"
		+ " Connect to text.\",\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/"
		+ "cornell/Top?id=10657875\"}\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testFindingAidURL() throws ClassNotFoundException, SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','2',"‡3 Finding aid ‡u http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107"));
		String expected =
		"url_findingaid_display: http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107|Finding aid\n" + 
		"notes_t: Finding aid\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}
	
	@Test
	public void testJustURL() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.marcHoldings.add(online);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡u http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		String expected =
		"url_access_json: {\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top"
		+ "?id=10657875\"}\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testBookplateURL() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		MarcRecord holdingRec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		holdingRec.id = "9489596";
		holdingRec.dataFields.add(new DataField(1,"856",'4',' ',
				"‡u http://plates.library.cornell.edu/donor/DNR00450 ‡z From the Estate of Charles A. Leslie."));
		bibRec.marcHoldings.add(holdingRec);
		String expected =
		"donor_t: From the Estate of Charles A. Leslie.\n"+
		"donor_s: DNR00450\n"+
		"url_bookplate_display: http://plates.library.cornell.edu/donor/DNR00450|From the Estate of Charles A. Leslie.\n"+
		"notes_t: From the Estate of Charles A. Leslie.\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void testAccessLinkThatMatchesOtherLinkPattern() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "10205060";
		bibRec.dataFields.add(new DataField(1,"856",'4',' ',
		"‡i dbcode=JTX; providercode=PRVAWR "+
		"‡u http://proxy.library.cornell.edu/login?url=https://www.taylorfrancis.com/books/e/9781466552609 "+
		"‡z Full text is available via download of individual chapter PDFs; scroll down for full table of contents."));
		bibRec.marcHoldings.add(online);
		String expected =
		"notes_t: Full text is available via download of individual chapter PDFs;"
		      + " scroll down for full table of contents.\n" + 
		"url_access_json: {\"providercode\":\"PRVAWR\",\"dbcode\":\"JTX\","
		                + "\"description\":\"Full text is available via download of individual chapter PDFs;"
		                                 + " scroll down for full table of contents.\","
		+ "\"url\":\"http://proxy.library.cornell.edu/login?url=https://www.taylorfrancis.com/books/e/9781466552609\"}\n" + 
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
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
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
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
		bibRec.marcHoldings.add(online);
		String expected =
		"url_other_display: http://www.loc.gov/catdir/enhancements/fy0667/2006046631-t.html|"
		+ "Available from the U.S. Government Printing Office. Table of contents only\n" + 
		"notes_t: Available from the U.S. Government Printing Office. Table of contents only\n" + 
		"notes_t: Available from the U.S. Government Printing Office.\n" + 
		"url_access_json: {\"providercode\":\"PRVLSH\",\"dbcode\":\"ACAJP\",\"description\":"
		+ "\"Available from the U.S. Government Printing Office.\","
		+ "\"url\":\"http://purl.access.gpo.gov/GPO/LPS77292\"}\n" + 
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void twoAccessLinksOneLooksLikePublishersWebsite() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "10758189";
		bibRec.dataFields.add(new DataField(1,"856",'4','0',
		"‡i dbcode=AAVGB; providercode=PRVJHQ "+
		"‡u http://resolver.library.cornell.edu/misc/10758189 "+
		"‡x http://proxy.library.cornell.edu/login?url=https://www.plumbsveterinarydrugs.com/auth "+
		"‡z Plumb's is currently in a rebuild of the website to be completed by the end of Q1 of 2021."
		+ " On campus access is working during this time. For off campus access, please contact vetref@cornell.edu"));
		bibRec.dataFields.add(new DataField(2,"856",'4','0', // this one matches publishers website criteria
		"‡u http://proxy.library.cornell.edu/login?url=https://academic.plumbs.com/ "+
		"‡z New Plumb's website as of April 2021. Off campus access to be working soon."));
		bibRec.marcHoldings.add(online);
		String expected =
		"notes_t: Plumb's is currently in a rebuild of the website to be completed by the end of Q1 of 2021."
		+ " On campus access is working during this time. For off campus access, please contact vetref@cornell.edu\n" + 
		"url_access_json: {\"providercode\":\"PRVJHQ\",\"dbcode\":\"AAVGB\","
		+ "\"description\":\"Plumb's is currently in a rebuild of the website to be completed by the end of Q1 of 2021."
		+     " On campus access is working during this time. For off campus access, please contact vetref@cornell.edu\","
		+ "\"url\":\"http://resolver.library.cornell.edu/misc/10758189\"}\n" + 
		"notes_t: New Plumb's website as of April 2021. Off campus access to be working soon.\n" + 
		"url_access_json: {\"description\":\"New Plumb's website as of April 2021. Off campus access to be working soon.\","
		+ "\"url\":\"http://proxy.library.cornell.edu/login?url=https://academic.plumbs.com/\"}\n" + 
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void nonTOUUseOf856i() throws IOException, ClassNotFoundException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "10204686";
		bibRec.dataFields.add(new DataField(1,"856",'7',' ',
		"‡3 Available from the U.S. Government Printing Office "+
		"‡i Archived issues "+
		"‡i dbcode=ACAJP; providercode=PRVLSH "+
		"‡u https://purl.fdlp.gov/GPO/gpo86434"));
		bibRec.marcHoldings.add(online);
		String expected =
		"notes_t: Available from the U.S. Government Printing Office\n" + 
		"url_access_json: {\"providercode\":\"PRVLSH\",\"dbcode\":\"ACAJP\","
		+ "\"description\":\"Available from the U.S. Government Printing Office\","
		+ "\"url\":\"https://purl.fdlp.gov/GPO/gpo86434\"}\n" + 
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

}
