package edu.cornell.library.integration.indexer.documentPostProcess;

import static org.junit.Assert.*;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;

public class SinglePubDateSortTest {

	@Test
	public void testForEmptyDoc() throws Exception {
		DocumentPostProcess dpp = new SinglePubDateSort();
		SolrInputDocument doc = new SolrInputDocument();	
		//this should not throw an exception when doc has nothing 
		dpp.p("notImportant", null, null, doc);
	}
	
	@Test
	public void testForEmptyPubDateField() throws Exception {
		DocumentPostProcess dpp = new SinglePubDateSort();
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("pub_date", null);
		//this should not throw an exception when doc has nothing 
		dpp.p("notImportant", null, null, doc);
	} 
	
	@Test
	public void testForMultiPubDate() throws Exception {
		DocumentPostProcess dpp = new SinglePubDateSort();
		SolrInputDocument doc = new SolrInputDocument();	
		doc.addField("pub_date","1999");
		doc.addField("pub_date","1987");
		doc.addField("pub_date","1990");		
		dpp.p("notImportant", null, null, doc);
		SolrInputField f = doc.get("pub_date_sort");
		assertNotNull(f);
		assertEquals(1, f.getValueCount());
	}

	@Test
	public void testForMultiPubDateSortToSingle() throws Exception {
		DocumentPostProcess dpp = new SinglePubDateSort();
		SolrInputDocument doc = new SolrInputDocument();	
		doc.addField("pub_date_sort","1999");
		doc.addField("pub_date_sort","1987");		
		dpp.p("notImportant", null, null, doc);
		SolrInputField f = doc.get("pub_date_sort");
		assertNotNull(f);
		assertEquals(1, f.getValueCount());
	}

	@Test
	public void testPubDateAndPubDateSort() throws Exception {
		DocumentPostProcess dpp = new SinglePubDateSort();
		SolrInputDocument doc = new SolrInputDocument();	
		doc.addField("pub_date_sort","1999");
		doc.addField("pub_date_sort","1993");
		doc.addField("pub_date","1400");		
		dpp.p("notImportant", null, null, doc);
		SolrInputField f = doc.get("pub_date_sort");
		assertNotNull(f);
		assertEquals(1, f.getValueCount());
		assertEquals("1400",f.getValue());
	}
}
