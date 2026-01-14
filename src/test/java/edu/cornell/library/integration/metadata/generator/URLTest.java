package edu.cornell.library.integration.metadata.generator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.Config;

public class URLTest {

	SolrFieldGenerator gen = new URL();
	static MarcRecord online ;
	static Map<String,Object> onlineFolioHolding ;
	static List<Map<String,Object>> onlineFolioHoldingList ;
	static Config config;
	private static String resourceDataJson = null;
	
	public static String loadResourceFile(String filename) throws IOException {
		try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
				Scanner s = new Scanner(is,"UTF-8")) {
			return s.useDelimiter("\\A").next();
		}
	}

	@BeforeClass
	public static void createServRemoHolding() throws IOException {
		resourceDataJson = loadResourceFile("example_reference_data/locations.json");
		SupportReferenceData.initializeLocations(resourceDataJson);
		String onlineLocId = SupportReferenceData.locations.getUuid("serv,remo");
		onlineFolioHolding = new HashMap<>();
		onlineFolioHolding.put("permanentLocationId", onlineLocId);
		onlineFolioHoldingList = new ArrayList<>();
		onlineFolioHoldingList.add(onlineFolioHolding);

		online = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
		online.id = "1";
		online.dataFields.add(new DataField(1,"852",' ',' ',"‡b serv,remo"));
	}

	@Test
	public void ebscoTitleLink() throws SQLException, IOException {
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
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void userLimitsIn899() throws SQLException, IOException {
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
			"availability_facet: url_access\n"+
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
			"availability_facet: url_access\n"+
			"online: Online\n";
			assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
		}
	}

	@Test   //8637892 DISCOVERYACCESS-2947
	public void testMultipleAccessWithDifferentTOU() throws IOException, SQLException {
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
		"availability_facet: url_access\n"+
		"notes_t: Full text available from Safari Technical Books Connect to text.\n"+
		"url_access_json: {\"providercode\":\"PRVPQU\",\"dbcode\":\"DRU\",\"description\":"
		+ "\"Full text available from Safari Technical Books Connect to text.\",\"ssid\":"
		+ "\"ssj0000907852\",\"url\":\"http://proxy.library.cornell.edu/login?url="
		+ "http://proquest.safaribooksonline.com/9781118529669\"}\n"+
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testNoTOU() throws IOException, SQLException {
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
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testFindingAidURL() throws SQLException, IOException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','2',"‡3 Finding aid ‡u http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107"));
		String expected =
		"url_findingaid_display: http://resolver.library.cornell.edu/cgi-bin/EADresolver?id=RMM08107|Finding aid\n" + 
		"notes_t: Finding aid\n"+
		"availability_facet: url_findingaid\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}
	
	@Test
	public void testJustURL() throws IOException, SQLException {
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.marcHoldings.add(online);
		rec.dataFields.add(new DataField(1,"856",'4','0',
				"‡u http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		String expected =
		"url_access_json: {\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top"
		+ "?id=10657875\"}\n"+
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );
	}

	@Test
	public void testBookplateURL() throws IOException, SQLException {

		List<Map<String,Object>> holdings = new ArrayList<>();
		{
			Map<String,String> link = new LinkedHashMap<>();
			link.put("uri","http://plates.library.cornell.edu/donor/DNR00450");
			link.put("publicNote", "From the Estate of Charles A. Leslie.");
			link.put("relationshipId", "5bfe1b7b-f151-4501-8cfa-23b321d5cd1e");
			List<Map<String,String>> links = new ArrayList<>();
			links.add(link);
			Map<String,Object> holding = new HashMap<>();
			holding.put("electronicAccess", links);
			holdings.add(holding);
		}

		String expected =
		"donor_t: From the Estate of Charles A. Leslie.\n"+
		"donor_display: From the Estate of Charles A. Leslie.\n"+
		"donor_s: DNR00450\n"+
		"availability_facet: url_bookplate\n";

		MarcRecord marc = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		marc.folioHoldings = holdings;
		assertEquals( expected, this.gen.generateSolrFields(marc, null).toString() );

		Map<String,Object> instance = new LinkedHashMap<>();
		instance.put("holdings", holdings);
		assertEquals( expected, this.gen.generateNonMarcSolrFields(instance, null).toString() );
	}

	@Test
	public void guardAgainstNullLinkHash() throws IOException, SQLException {
		List<Map<String,Object>> holdings = new ArrayList<>();
		{
			Map<String,String> link = null;
			List<Map<String,String>> links = new ArrayList<>();
			links.add(link);
			Map<String,Object> holding = new HashMap<>();
			holding.put("electronicAccess", links);
			holding.put("permanentLocationId", SupportReferenceData.locations.getUuid("serv,remo"));
			holdings.add(holding);
		}

		MarcRecord marc = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		marc.folioHoldings = holdings;
		assertEquals( "", this.gen.generateSolrFields(marc, null).toString() );

	}

	@Test
	public void testAccessLinkThatMatchesOtherLinkPattern() throws IOException, SQLException {
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
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void otherContentLinkPrintBook() throws IOException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "9142371";
		bibRec.dataFields.add(new DataField(1,"856",'7',' ',
		"‡3 View cover art "+
		"‡u http://midwesttapes.com/images/movies/000/000/000/011/353/000000000011353737.jpg ‡2 http"));
//		System.out.println(gen.generateSolrFields(bibRec, null).toString());
		String expected =
		"url_other_display: http://midwesttapes.com/images/movies/000/000/000/011/353/000000000011353737.jpg|View cover art\n" + 
		"notes_t: View cover art\n"+
		"availability_facet: url_other\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void otherContentLinkOnlineBook() throws IOException, SQLException {
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
		"availability_facet: url_other\n"+
		"notes_t: Available from the U.S. Government Printing Office.\n" + 
		"url_access_json: {\"providercode\":\"PRVLSH\",\"dbcode\":\"ACAJP\",\"description\":"
		+ "\"Available from the U.S. Government Printing Office.\","
		+ "\"url\":\"http://purl.access.gpo.gov/GPO/LPS77292\"}\n" + 
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void twoAccessLinksOneLooksLikePublishersWebsite() throws IOException, SQLException {
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
		"availability_facet: url_access\n"+
		"notes_t: New Plumb's website as of April 2021. Off campus access to be working soon.\n" + 
		"url_access_json: {\"description\":\"New Plumb's website as of April 2021. Off campus access to be working soon.\","
		+ "\"url\":\"http://proxy.library.cornell.edu/login?url=https://academic.plumbs.com/\"}\n" + 
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void nonTOUUseOf856i() throws IOException, SQLException {
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
		"availability_facet: url_access\n"+
		"online: Online\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void instanceURL() throws IOException {

		List<Map<String,String>> links = new ArrayList<>();
		{
		Map<String,String> link = new LinkedHashMap<>();
		link.put("uri","http://proxy.library.cornell.edu/login?url=https://www.berghahnjournals.com/view/journals/turba/turba-overview.xml?rskey=scYRra&result=44 ");
		link.put("materialsSpecification", "2022 - Present");
		link.put("relationshipId", "f5d0068e-6272-458e-8a81-b85e7b9a14aa");
		links.add(link);
		}
		{
		Map<String,String> link = new LinkedHashMap<>();
		link.put("uri","http://pda.library.cornell.edu/coutts/pod.cgi?CouttsID=cou37972731");
		link.put("linkText", "");
		link.put("materialsSpecification", "");
		link.put("relationshipId", "f50c90c9-bae0-4add-9cd0-db9092dbc9dd");
		link.put("publicNote", "Click to ask Cornell University Library to RUSH purchase. We will contact you by email when it arrives (typically within a week)");
		links.add(link);
		}
		Map<String,Object> instance = new LinkedHashMap<>();
		instance.put("electronicAccess", links);
		instance.put("holdings", onlineFolioHoldingList);
		String expected =
		"notes_t: 2022 - Present\n"+
		"url_access_json: {\"description\":\"2022 - Present\",\"url\":\"http://proxy.library.cornell.edu/login?url=https://www.berghahnjournals.com/view/journals/turba/turba-overview.xml?rskey=scYRra&result=44\"}\n"+
		"availability_facet: url_access\n"+
		"url_pda_display: http://pda.library.cornell.edu/coutts/pod.cgi?CouttsID=cou37972731|Click to ask Cornell University Library to RUSH purchase. We will contact you by email when it arrives (typically within a week)\n" + 
		"notes_t: Click to ask Cornell University Library to RUSH purchase. We will contact you by email when it arrives (typically within a week)\n"+
		"availability_facet: url_pda\n"+
		"online: Online\n";
		assertEquals(expected,this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void invalidSyntaxInFolioURI() throws IOException {
		List<Map<String,Object>> holdings = new ArrayList<>();
		{
			List<Map<String,String>> links = new ArrayList<>();
			{
				Map<String,String> link = new LinkedHashMap<>(); // holding 15279606
				link.put("uri",";;c\\nhttp://plates.library.cornell.edu/donor/DNR00393/");
				link.put("publicNote", "A Gift of Il Hwan Cho and Soon Ja Cho in support of Korean Studies");
				link.put("relationshipId", "5bfe1b7b-f151-4501-8cfa-23b321d5cd1e");
				links.add(link);
			}
			{
				Map<String,String> link = new LinkedHashMap<>(); // holding 15288623
				link.put("uri","\\nhttp://plates.library.cornell.edu/donor/DNR00401/\\n\\nhttp://plates.library.cornell.edu/donor/DNR00401/\\n\\nhttp://plates.library.cornell.edu/donor/DNR00401/\\nhttp://plates.library.cornell.edu/donor/DNR00401/");
				link.put("publicNote", "The Jarett F. and Younghee Kim Wait Fund for Korean Literature");
				link.put("relationshipId", "5bfe1b7b-f151-4501-8cfa-23b321d5cd1e");
				links.add(link);
			}
			Map<String,Object> holding = new HashMap<>();
			holding.put("electronicAccess", links);
			holdings.add(holding);
		}
		Map<String,Object> instance = new LinkedHashMap<>();
		instance.put("holdings", holdings);
		assertEquals("",this.gen.generateNonMarcSolrFields(instance, null).toString());
	}

	@Test
	public void syntaxValidationInMarcURI() throws IOException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "123456";
		bibRec.dataFields.add(new DataField(1,"856",'7',' ',
		"‡3 Connect to full text. "+
		"‡u NOT A  LINK"));
		bibRec.dataFields.add(new DataField(2,"856",'4','2',//15459111
		"‡a Rose Goldsen Archive of New Media Art "+
		"‡u http:// goldsen.library.cornell.edu"));
		bibRec.dataFields.add(new DataField(3,"856",'4',' ',//15157320
		"‡3 Image "+ // This one should be okay with selective URL encoding of the brackets {}
		"‡u https://img1.od-cdn.com/ImageType-100/3082-1/{04999A8F-6BC6-4370-B438-EE0C49D8C921}Img100.jpg"));
		bibRec.dataFields.add(new DataField(4,"856",'7',' ',
		"‡3 Connect to full text. "+
		"‡u http://1.2.3.4/ipaddress-accepted-as-host.html"));
		bibRec.folioHoldings = onlineFolioHoldingList;
		assertEquals( 
				"notes_t: Image\n" + 
				"url_access_json: {\"description\":\"Image\","
				+ "\"url\":\"https://img1.od-cdn.com/ImageType-100/3082-1/{04999A8F-6BC6-4370-B438-EE0C49D8C921}Img100.jpg\"}\n" + 
				"availability_facet: url_access\n"+
				"notes_t: Connect to full text.\n" + 
				"url_access_json: {\"description\":\"Connect to full text.\","
				+ "\"url\":\"http://1.2.3.4/ipaddress-accepted-as-host.html\"}\n"+
				"availability_facet: url_access\n"+
				"online: Online\n",
				this.gen.generateSolrFields(bibRec, null).toString() );
	}

	@Test
	public void encodeURLs() {
		assertEquals(
				"http://sunsite2.berkeley.edu:8000/%22target=%22_blank",
				URL.selectivelyUrlEncode("http://sunsite2.berkeley.edu:8000/\"target=\"_blank"));
		
	}

	@Test
	public void suppressedHoldingAccessLink() throws IOException, SQLException {
		Map<String, String> accessLink = new TreeMap<>();
		accessLink.put("uri", "https://bogus.com/bogus");
		accessLink.put("publicNote", "Test");
		accessLink.put("relationshipId", "00000000-00000-0000-0000-000000000000");

		List<Map<String, String>> electronicAccess = new ArrayList<>();
		electronicAccess.add(accessLink);

		Map<String, Object> folioHolding = new TreeMap<>();
		folioHolding.put("permanentLocationId", "2e19230b-aeeb-4419-92cb-853611611718");
		folioHolding.put("electronicAccess", electronicAccess);

		List<Map<String, Object>> folioHoldings = new ArrayList<>();
		folioHoldings.add(folioHolding);

		// un-suppressed
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.dataFields.add(new DataField(1,"856",'4','0',
		"‡i 2471499 "+
		"‡y Click here to find online versions of this title. "+
		"‡u https://search.ebscohost.com/login.aspx?CustID=s9001366&db=edspub&type=44&"
		+ "bQuery=AN%202471499&direct=true&site=pfi-live"));
		rec.folioHoldings = folioHoldings;
		StringBuilder expected = new StringBuilder("ebsco_title_facet: 2471499\n")
				.append("notes_t: Test\n")
				.append("url_access_json: {")
				.append("\"description\":\"Test\",")
				.append("\"url\":\"https://bogus.com/bogus\"}\n")
				.append("availability_facet: url_access\n")
				.append("notes_t: Click here to find online versions of this title.\n")
				.append("url_access_json: {")
				.append("\"titleid\":\"2471499\",")
				.append("\"description\":\"Click here to find online versions of this title.\",")
				.append("\"url\":\"https://search.ebscohost.com/login.aspx?CustID=s9001366&db=edspub&type=44&")
				.append(          "bQuery=AN%202471499&direct=true&site=pfi-live\"}\n")
				.append("availability_facet: url_access\n")
				.append("online: Online\n");
		assertEquals( expected.toString(), this.gen.generateSolrFields(rec, null).toString() );

		// suppressed
		folioHolding.put("discoverySuppress", true);
		expected = new StringBuilder("ebsco_title_facet: 2471499\n")
				.append("notes_t: Click here to find online versions of this title.\n")
				.append("url_access_json: {")
				.append("\"titleid\":\"2471499\",")
				.append("\"description\":\"Click here to find online versions of this title.\",")
				.append("\"url\":\"https://search.ebscohost.com/login.aspx?CustID=s9001366&db=edspub&type=44&")
				.append(          "bQuery=AN%202471499&direct=true&site=pfi-live\"}\n")
				.append("availability_facet: url_access\n")
				.append("online: Online\n");
		assertEquals( expected.toString(), this.gen.generateSolrFields(rec, null).toString() );
	}


	@Test
	public void accessLinkInSuppressedHolding() throws IOException, SQLException {
		// un-suppressed
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		List<Map<String,Object>> holdings = new ArrayList<>();
		rec.folioHoldings = holdings;
		{
			Map<String,String> link = new LinkedHashMap<>();
			link.put("uri","https://libraryhelp.kanopy.com/hc/en-us/articles/1500003981461");
			link.put("publicNote", "Licensing includes Public Performance Rights (PPR). The film can be screened to "
					+ "large gatherings as long as admission is not charged.");
			link.put("relationshipId", "3b430592-2e09-4b48-9a0c-0636d66b9fb3");
			List<Map<String,String>> links = new ArrayList<>();
			links.add(link);
			Map<String,Object> holding = new HashMap<>();
			holding.put("electronicAccess", links);
			holdings.add(holding);
		}

		// If the holding isn't suppressed, the link is recognized. (As non-access link bec. no serv,remo is available)
		String expected =
		"url_other_display: https://libraryhelp.kanopy.com/hc/en-us/articles/1500003981461|Licensing includes Public "
		+ "Performance Rights (PPR). The film can be screened to large gatherings as long as admission is not charged.\n" +
		"notes_t: Licensing includes Public Performance Rights (PPR). The film can be screened to large gatherings as "
		+ "long as admission is not charged.\n"+
		"availability_facet: url_other\n";
		assertEquals( expected, this.gen.generateSolrFields(rec, null).toString() );

		// If the holding is suppressed, links it contains aren't recognized at all.
		rec.folioHoldings.get(0).put("discoverySuppress", true);
		System.out.println(this.gen.generateSolrFields(rec, null).toString());
		assertEquals( "", this.gen.generateSolrFields(rec, null).toString() );
	}


	@Test
	public void tocview() throws IOException, SQLException {
		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		bibRec.id = "10204686";
		bibRec.dataFields.add(new DataField(1,"856",' ',' ',
		"‡u http://tocview.library.cornell.edu/viewer/bib/1084136 ‡z Link to table of Contents Images"));
		bibRec.marcHoldings.add(online);
		String expected =
		"url_tocview_display: http://tocview.library.cornell.edu/viewer/bib/1084136|Link to table of Contents Images\n"+
		"notes_t: Link to table of Contents Images\n"+
		"availability_facet: url_tocview\n";
		assertEquals( expected, this.gen.generateSolrFields(bibRec, null).toString() );
	}

}
