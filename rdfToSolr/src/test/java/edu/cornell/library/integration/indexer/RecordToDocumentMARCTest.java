package edu.cornell.library.integration.indexer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This tests the RecordToDocumentMARC class.  The test will
 * setup a temp solr service, setup a temp rdf store, load the 
 * store wit some records, and then use RecordToDocumentMARC
 * to index solr documents for the records.
 * 
 * Once this is done test queries can be run against the solr
 * service.
 * 
 * @author bdc34 
 */
public class RecordToDocumentMARCTest extends SolrLoadingTestBase {
	
	@BeforeClass
	public static void setup() throws Exception{
		setupSolr();		
	}

	@AfterClass
	public static void down() throws Exception{
		takeDownSolr();
	}
	
	@Test 
	public void testForGoodStartup() throws Exception{
		super.testSolrWasStarted();
		super.testRadioactiveIds();
		super.testLanguageMappingsInRDF();
		super.testCallnumberMappingsInRDF();
	}	
	
	@Test
	public void testBronte() throws SolrServerException{
		
		/* make sure that we have the document in the index before we do anything */			
		testQueryGetsDocs("Expect to find Bronte document by doc:id 4696",
				new SolrQuery().setQuery("id:4696").setParam("qt", "standard"),
				new String[]{ "4696" } ) ;			
		
		testQueryGetsDocs("Expect to find doc:id 4696 when searching for 'bronte'",
				new SolrQuery().setQuery("bronte"),
				new String[]{ "4696" } ) ;
				
		testQueryGetsDocs("Expect to find doc:id 4696 when searching for 'Selected Bronte\u0308 poems'",
				new SolrQuery().setQuery( "Selected Bronte\u0308 poems") ,
				new String[]{ "4696" } ) ;
				
		testQueryGetsDocs("Expect to find doc:id 4696 when searching for 'Selected Bronte\u0308 poems' all in quotes",
				new SolrQuery().setQuery( "\"Selected Bronte\u0308 poems\"") ,
				new String[]{ "4696" } ) ;
	}		
	
	@Test
	public void testLanguageSearchFacet() throws SolrServerException{

		try {
			InputStream is = rdf.sparqlSelectQuery("SELECT * WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4696> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f." +
					"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"008\". " +
					"?f <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?val. " +
					"?l <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/Language>. " +
					"?l <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/code> ?langcode. " +
					"FILTER( SUBSTR( ?val,36,3) = ?langcode ) " +
					"?l <http://www.w3.org/2000/01/rdf-schema#label> ?language.}",RDFService.ResultFormat.TEXT);
			String language = convertStreamToString(is);
			assertTrue("Record was expected to be mappable to English language.",language.contains("English"));
		} catch (RDFServiceException e) {
			assertTrue("failed to query rdf service",false);
		}

		
		//"Expected English for language_facet on document 4696, " +
		//"it is likely the language mapping RDF is not loaded.",
		SolrQuery q = new SolrQuery().setQuery("bronte");
		QueryResponse resp = solr.query(q);				
		SolrDocumentList sdl = resp.getResults();
		assertNotNull("expected to find doc 4696", sdl);		
		assertEquals(1, sdl.size());		
		
		FacetField ff = resp.getFacetField("language_facet");		
		assertNotNull( ff );		
		boolean englishFound = false;
		boolean englishGTEOne = false;
		for( Count ffc : ff.getValues()){
			if( "English".equals( ffc.getName() )){
				englishFound = true;
				englishGTEOne = ffc.getCount() >= 1;
			}
		}
		assertTrue("English should be a facet", englishFound);
		assertTrue("doc 4696 should be English", englishGTEOne);
	}
	
	@Test
	public void testCallnumSearchFacet() throws SolrServerException{
		
		//"Expected "P - Language & Literature" for lc_1letter_facet on document 4696."
		SolrQuery q = new SolrQuery().setQuery("bronte");
		QueryResponse resp = solr.query(q);				
		SolrDocumentList sdl = resp.getResults();
		assertNotNull("expected to find doc 4696", sdl);		
		assertEquals(1, sdl.size());		
		
		FacetField ff = resp.getFacetField("lc_1letter_facet");		
		assertNotNull( ff );		
		boolean pFound = false;
		boolean pGTEOne = false;
		for( Count ffc : ff.getValues()){
			if( "P - Language & Literature".equals( ffc.getName() )){
				pFound = true;
				pGTEOne = ffc.getCount() >= 1;
			}
		}
		assertTrue("P - Language & Literature should be a facet", pFound);
		assertTrue("doc 4696 should be P - Language & Literature", pGTEOne);
	}

	public static String convertStreamToString(java.io.InputStream is) {
	    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
	    return s.hasNext() ? s.next() : "";
	}
}
