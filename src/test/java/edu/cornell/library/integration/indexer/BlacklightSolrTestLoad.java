package edu.cornell.library.integration.indexer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;


/**
 * This isn't really a unit test. This is more of an integration test that uses 
 * a pre-existing solr test server.
 * 
 * TODO: Give this class a main that does the needed testing.
 * 
 *
 */
public class BlacklightSolrTestLoad extends RdfLoadingTestBase {
			
	static SolrClient solr = null;		
		
	/**
	 * These are the path prefixes to try to use 
	 * when looking for RDF directories. The intent
	 * is to allow the test to be run from different 
	 * directories.  It would be better to load these
	 * from resources but you cannot access directories
	 * from resources.  
	 * 
	 * A better system for this would be welcome. The
	 * test need to be run from the build script
	 * and from eclipse. 
	 */
	static final String[] testRDFDirBases = {
		"./",
		"../"
	};
	
	static final String fallbackSolrDir1 = new File("../solr/corex").getAbsolutePath() ;
	static final String fallbackSolrDir2 = new File("./solr/corex").getAbsolutePath() ;
	
	
	
	@BeforeClass
	public static void setup() throws Exception{
		setupSolr();		
	}


	@Test
	public void testForGoodStartup() throws Exception{
		super.testLanguageMappingsInRDF();
		super.testCallnumberMappingsInRDF();
	}	
	
    public static TemporaryFolder solrTmpFolder = null;
		
	public static void setupSolr() throws Exception{		
		setupRdf();
		solr = new HttpSolrClient("http://da-dev-solr.library.cornell.edu/solr/test");
		indexStandardTestRecords( solr, rdf );		
	}
	
	public static void takeDownSolr() throws Exception{		
		solrTmpFolder.delete();		
	}
	
	public void testSolrWasStarted() throws SolrServerException, IOException {
		assertNotNull( solr );
		solr.ping();
	}
	
	public void testRadioactiveIds() throws SolrServerException, IOException{	
		String[] ids = new String[]{				
				"UNTRadMARC001", 		
				"UNTRadMARC002",
				"UNTRadMARC003",
				"UNTRadMARC004",
				"UNTRadMARC005",
				"UNTRadMARC006",
				"UNTRadMARC007",
				"UNTRadMARC008",
				"UNTRadMARC009",
				"UNTRadMARC010"};
		
		for(String radId : ids){
			SolrQuery query = new SolrQuery();	
			query.setQuery("id:" + radId);
			query.setParam("qt", "standard");
			String[] id = { radId };
			testQueryGetsDocs(
				"Making sure all Radioactive MARC can be found by id",
				query,  id);
		}
	}
	
	
			
	public void testLanguageMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://da-rdf.library.cornell.edu/individual/leng>";
		assertTrue("Expected to find statements about English mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery(
						"ASK WHERE { " 
						+ englishURI + " <http://da-rdf.library.cornell.edu/integrationLayer/0.1/code> ?a }"));				
	}
	
	
	/** 
	 * Test that a document with the given IDs are in the results for the query. 
	 * @throws SolrServerException 
	 * @throws IOException */
	void testQueryGetsDocs(String errmsg, SolrQuery query, String[] docIds) throws SolrServerException, IOException{
		assertNotNull(errmsg + " but query was null", query);
		assertNotNull(errmsg + " but docIds was null", docIds );
									
		QueryResponse resp = solr.query(query);
		if( resp == null )
			fail( errmsg + " but Could not get a solr response");
		
		Set<String> expecteIds = new HashSet<String>(Arrays.asList( docIds ));
		for( SolrDocument doc : resp.getResults()){
			assertNotNull(errmsg + ": solr doc was null", doc);
			String id = (String) doc.getFirstValue("id");
			assertNotNull(errmsg+": no id field in solr doc" , id);
			expecteIds.remove( id );			
		}
		if( expecteIds.size() > 0){
			String errorMsg = 
					"\nThe query '"+ query + "' was expected " +
					"to return the following ids but did not:";
			for( String id : expecteIds){
				errorMsg= errorMsg+"\n" + id;
			}					
			
			fail( errmsg + errorMsg);
		}
	}
	
	public static String convertStreamToString(java.io.InputStream is) {
	    @SuppressWarnings("resource")
		java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
	    return s.hasNext() ? s.next() : "";
	}

	private static void indexStandardTestRecords( SolrClient solr , RDFService rdfService) throws Exception {
		RecordToDocument r2d = new RecordToDocumentMARC();
		InputStream is = rdf.sparqlSelectQuery("SELECT * WHERE { ?bib  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" +
				"             <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> .}",
				 RDFService.ResultFormat.TEXT);
		String bib_xml = convertStreamToString(is);
		System.out.println(bib_xml);
		Pattern p = Pattern.compile("<([^>]*)>");
		Matcher m = p.matcher(bib_xml);
		SolrBuildConfig config = SolrBuildConfig.loadConfig( new String[2] );

		while (m.find()) {
			String uri = m.group(1);
			System.out.println("*** " + uri + " ***");
			SolrInputDocument doc;
			try {
				doc = r2d.buildDoc(uri, config);
			} catch (Exception e) {
				System.out.println("failed on uri:" + uri);
				e.printStackTrace();
				throw e;
			}
			System.out.println( IndexingUtilities.prettyXMLFormat( ClientUtils.toXML( doc ) ) );
			try {
				solr.add( doc );
			} catch (Exception e) {
				System.out.println("Failed adding doc to solr for uri:" + uri);				
				System.out.println( IndexingUtilities.toString( doc ) + "\n\n" );
//				throw e;
			}
		}
		solr.commit();
	}
	
	
	
	//from http://stackoverflow.com/questions/3861989/preferred-way-of-loading-resources-in-java
	public static URL getResource(String resource){
	    URL url ;

	    //Try with the Thread Context Loader. 
	    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	    if(classLoader != null){
	        url = classLoader.getResource(resource);
	        if(url != null){
	            return url;
	        }
	    }

	    //Let's now try with the classloader that loaded this class.
//	    classLoader = getClass().getClassLoader();
//	    if(classLoader != null){
//	        url = classLoader.getResource(resource);
//	        if(url != null){
//	            return url;
//	        }
//	    }

	    classLoader = ClassLoader.getSystemClassLoader();
	    if(classLoader != null){
	        url = classLoader.getResource(resource);
	        if(url != null){
	            return url;
	        }
	    }	    
	    
	    //Last ditch attempt. Get the resource from the classpath.
	    return ClassLoader.getSystemResource(resource);
	}

}
