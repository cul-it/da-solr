package edu.cornell.library.integration.indexer.resultSetToFields;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.Subfield;

@SuppressWarnings("static-method")
public class URLTest {

	@Test
	public void testMultipleAccessWithDifferentTOU() throws IOException { //8637892 DISCOVERYACCESS-2947
		MarcRecord rec = new MarcRecord(null);
		DataField df = new DataField(1,"856");
		df.subfields.add(new Subfield(1, '3', "Full text available from Ebrary The Arts Subscription Collection"));
		df.subfields.add(new Subfield(2, 'i', "ssid=ssj0000907852; dbcode=AAGPP; providercode=PRVAHD"));
		df.subfields.add(new Subfield(3, 'u', "http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		df.subfields.add(new Subfield(4, 'z', "Connect to text."));
		rec.dataFields.add(df);
		df = new DataField(2,"856");
		df.subfields.add(new Subfield(1, '3', "Full text available from Safari Technical Books"));
		df.subfields.add(new Subfield(2, 'i', "ssid=ssj0000907852; dbcode=DRU; providercode=PRVPQU"));
		df.subfields.add(new Subfield(3, 'u', "http://proxy.library.cornell.edu/login?url=http://proquest.safaribooksonline.com/9781118529669"));
		df.subfields.add(new Subfield(4, 'z', "Connect to text."));
		rec.dataFields.add(df);
		List<SolrField> allSolrFields = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			URL.SolrFieldValueSet vals = URL.generateSolrFields(fs);
			allSolrFields.addAll(vals.fields);
		}
		assertEquals(6, allSolrFields.size());
		assertEquals("url_access_display",allSolrFields.get(0).fieldName);
		assertEquals("notes_t",           allSolrFields.get(1).fieldName);
		assertEquals("url_access_json",   allSolrFields.get(2).fieldName);
		assertEquals("url_access_display",allSolrFields.get(3).fieldName);
		assertEquals("notes_t",           allSolrFields.get(4).fieldName);
		assertEquals("url_access_json",   allSolrFields.get(5).fieldName);
		assertEquals("http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"
				+ "|Full text available from Ebrary The Arts Subscription Collection Connect to text.",
				allSolrFields.get(0).fieldValue);
		assertEquals("Full text available from Ebrary The Arts Subscription Collection Connect to text.",
				allSolrFields.get(1).fieldValue);
		assertEquals("{\"providercode\":\"PRVAHD\",\"dbcode\":\"AAGPP\","
				+ "\"description\":\"Full text available from Ebrary The Arts Subscription Collection Connect to text.\","
				+ "\"ssid\":\"ssj0000907852\","
				+ "\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875\"}",
				allSolrFields.get(2).fieldValue);
		assertEquals("http://proxy.library.cornell.edu/login?url=http://proquest.safaribooksonline.com/9781118529669"
				+ "|Full text available from Safari Technical Books Connect to text.",
				allSolrFields.get(3).fieldValue);
		assertEquals("Full text available from Safari Technical Books Connect to text.",
				allSolrFields.get(4).fieldValue);
		assertEquals("{\"providercode\":\"PRVPQU\",\"dbcode\":\"DRU\","
				+ "\"description\":\"Full text available from Safari Technical Books Connect to text.\","
				+ "\"ssid\":\"ssj0000907852\","
				+ "\"url\":\"http://proxy.library.cornell.edu/login?url=http://proquest.safaribooksonline.com/9781118529669\"}",
				allSolrFields.get(5).fieldValue);
	}

	@Test
	public void testNoTOU() throws IOException {
		MarcRecord rec = new MarcRecord(null);
		DataField df = new DataField(1,"856");
		df.subfields.add(new Subfield(1, '3', "Full text available from Ebrary The Arts Subscription Collection"));
		df.subfields.add(new Subfield(3, 'u', "http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		df.subfields.add(new Subfield(4, 'z', "Connect to text."));
		rec.dataFields.add(df);
		List<SolrField> allSolrFields = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			URL.SolrFieldValueSet vals = URL.generateSolrFields(fs);
			allSolrFields.addAll(vals.fields);
		}
		assertEquals(3, allSolrFields.size());
		assertEquals("url_access_display",allSolrFields.get(0).fieldName);
		assertEquals("notes_t",           allSolrFields.get(1).fieldName);
		assertEquals("url_access_json",   allSolrFields.get(2).fieldName);
		assertEquals("http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"
				+ "|Full text available from Ebrary The Arts Subscription Collection Connect to text.",
				allSolrFields.get(0).fieldValue);
		assertEquals("Full text available from Ebrary The Arts Subscription Collection Connect to text.",
				allSolrFields.get(1).fieldValue);
		assertEquals("{\"description\":\"Full text available from Ebrary The Arts Subscription Collection Connect to text.\","
				+ "\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875\"}",
				allSolrFields.get(2).fieldValue);
	}
	@Test
	public void testJustURL() throws IOException {
		MarcRecord rec = new MarcRecord(null);
		DataField df = new DataField(1,"856");
		df.subfields.add(new Subfield(1, 'u', "http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875"));
		rec.dataFields.add(df);
		List<SolrField> allSolrFields = new ArrayList<>();
		for (DataFieldSet fs : rec.matchAndSortDataFields()) {
			URL.SolrFieldValueSet vals = URL.generateSolrFields(fs);
			allSolrFields.addAll(vals.fields);
		}
		assertEquals(2, allSolrFields.size());
		assertEquals("url_access_display",allSolrFields.get(0).fieldName);
		assertEquals("url_access_json",   allSolrFields.get(1).fieldName);
		assertEquals("http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875",
				allSolrFields.get(0).fieldValue);
		assertEquals("{\"url\":\"http://proxy.library.cornell.edu/login?url=http://site.ebrary.com/lib/cornell/Top?id=10657875\"}",
				allSolrFields.get(1).fieldValue);
	}

}
