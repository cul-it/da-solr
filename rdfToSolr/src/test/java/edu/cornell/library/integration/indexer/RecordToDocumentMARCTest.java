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
public class RecordToDocumentMARCTest {
	static SolrServer solr = null;	
	static RDFService rdf  = null;
	
	/**
	 * This test needs to load a set of RDF to use to build
	 * documents from. 
	 */
	static final String testRDFDir =  "rdf/testRecords/";
	
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
	
	static final String[] rMarcURIS = {
		"http://fbw4-dev.library.cornell.edu/individuals/b4696",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC001",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC002",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC003",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC004",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC005",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC006",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC007",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC008",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC009",
			"http://fbw4-dev.library.cornell.edu/individuals/bUNTRadMARC010"
	};
	
	static final String[] standardFiles = { "language_code.nt", "library.nt" };
	//static final String[] standardFiles = { "library.nt" };
	
    public static TemporaryFolder folder = null;
	
	@BeforeClass
	public static void setup() throws Exception{
		folder = new TemporaryFolder();
		folder.create();
		
		String solrTemplateDir = getSolrTemplateDir();
		if( solrTemplateDir == null)
			throw new Exception("could not find solr template directory");					
		
		File solrBase = prepareTmpSolrDir( solrTemplateDir, folder );
		solr = setupSolrIndex( solrBase );		
		indexStandardTestRecords( solr );		
	}

	@AfterClass
	public static void down() throws Exception{
		folder.delete();
	}
	
	@Test
	public void testSolrWasStarted() throws SolrServerException, IOException {
		assertNotNull( solr );
		solr.ping();
	}

	@Test
	public void testRadioactiveIds() throws SolrServerException{
	
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
	public void testLanguageMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://fbw4-dev.library.cornell.edu/individuals/leng>";
		assertTrue("Expected to find statements about English mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery("ASK WHERE { " + englishURI + " <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/code> ?a }"));
		
		InputStream is = rdf.sparqlSelectQuery("SELECT * WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4696> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f }",RDFService.ResultFormat.TEXT);
		String result = convertStreamToString( is );
		System.out.println(result);
		is = rdf.sparqlSelectQuery("SELECT * WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4696> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f." +
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"008\". }",RDFService.ResultFormat.TEXT);
		System.out.println(convertStreamToString(is));
		is = rdf.sparqlSelectQuery("SELECT * WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4696> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f." +
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"008\". " +
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?val. " +
				"?l <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/Language>.}",RDFService.ResultFormat.TEXT);
		System.out.println(convertStreamToString(is));
		is = rdf.sparqlSelectQuery("SELECT * WHERE { <http://fbw4-dev.library.cornell.edu/individuals/b4696> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f." +
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"008\". " +
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?val. " +
				"?l <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/Language>. " +
				"?l <http://fbw4-dev.library.cornell.edu/integrationLayer/code> ?langcode.}",RDFService.ResultFormat.TEXT);
		System.out.println(convertStreamToString(is));
		is = rdf.sparqlSelectQuery("SELECT * WHERE { "+englishURI + " ?p ?o }",RDFService.ResultFormat.TEXT);
		System.out.println(convertStreamToString(is));
		
	}
	
	public static String convertStreamToString(java.io.InputStream is) {
	    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
	    return s.hasNext() ? s.next() : "";
	}
	
	@Test
	public void testLanguageSearchFacet() throws SolrServerException{

		//"Expected English for language_facet on document 4696, " +
		//"it is likely the language mapping RDF is not loaded.",
		SolrQuery q = new SolrQuery().setQuery("bronte");
		QueryResponse resp = solr.query(q);				
		System.out.println( resp.toString() );
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
	
	/** 
	 * Test that a document with the given IDs are in the results for the query. 
	 * @throws SolrServerException */
	void testQueryGetsDocs(String errmsg, SolrQuery query, String[] docIds) throws SolrServerException{
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
	
	/**
	 * I was hoping to do this in a better way but right
	 * now the solr template directory is just guessed based
	 * on the CWD.
	 * 
	 * First attempt: get the value from a resource file.
	 * (didn't work out)
	 * Second attempt: guess based on CWD
	 * @return
	 * @throws IOException 
	 */
	private static String getSolrTemplateDir() throws IOException {
		String key = "solrTemplateDirectory";
		System.out.println( new File(".").getAbsolutePath() );
		//get the testSolr.properties file and load it
		URL url = getResource("/testSolr.properties");
		Properties props = new Properties();
		if (null != url) {            
            InputStream in = url.openStream();
            props = new Properties();
            props.load(in);           
		}

		String solrTemplateDir = props.getProperty(key);
		if( solrTemplateDir == null){
			//could not find properties, just guess.					
			solrTemplateDir = fallbackSolrDir1;			
		}
		if( !(new File( solrTemplateDir).exists() )){
			solrTemplateDir = fallbackSolrDir2;
		}
		return solrTemplateDir;		
	}

	public static File prepareTmpSolrDir(String solrTemplateDir, TemporaryFolder folder) throws IOException{
		File base = folder.newFolder("recordToDocumentMARCTest_solr");
		System.out.println( solrTemplateDir );
		FileUtils.copyDirectory(new File(solrTemplateDir), base );				
		return base;
	}
	
	public static SolrServer setupSolrIndex(File solrBase) throws ParserConfigurationException, IOException, SAXException{
		System.setProperty("solr.solr.home", solrBase.getAbsolutePath());
		CoreContainer.Initializer initializer = new CoreContainer.Initializer();
		CoreContainer coreContainer = initializer.initialize();
		return new EmbeddedSolrServer(coreContainer, "");
		
		//for a core use something like:
//	    File f = new File( solrBase, "solr.xml" );
//	    CoreContainer container = new CoreContainer();
//	    container.load( solrBase.getAbsolutePath(), f );
//	    return new EmbeddedSolrServer( container, "core1" );
	}
		
	private static void indexStandardTestRecords(SolrServer solr2) throws RDFServiceException, Exception {
		//load an RDF file of the radioactive MARC into a triple store 
		rdf = loadRMARC();
		
		//and index solr documents for them.  
		indexRdf();
	}

	private static RDFService loadRMARC() throws Exception {		
		//find test dir
		File testRDFDir = findTestDir();
		
		//load all files in test dir
		List<File> files = new LinkedList<File>(Arrays.asList(testRDFDir.listFiles()));		
		assertNotNull("no test RDF files found",files);
		assertTrue("no test RDF files found", files.size() > 0 );
		
		//load the standard files
		for(String fileName : standardFiles){
			files.add( new File( testRDFDir.getParentFile().getAbsolutePath() + File.separator +fileName));
		}
		
		Model model = ModelFactory.createDefaultModel();
		for (File file : files ){
			InputStream in = FileManager.get().open( file.getAbsolutePath() );	
			assertNotNull("Could not load file " + file.getAbsolutePath(), in );
			try{
				model.read(in,null,"N-TRIPLE");
			}catch(Throwable th){
				throw new Exception( "Could not load file " + file.getAbsolutePath() , th);
			}finally{
				in.close();
			}
		}				
		
		
		return new RDFServiceModel( model );
	}
	
	private static File findTestDir() {
		List<String> attempted = new ArrayList<String>();		
		File f = null;
		for( String base: testRDFDirBases){
			String attemptedDir =base + testRDFDir ;
			attempted.add( attemptedDir);
			f = new File( attemptedDir );
			if( f != null && f.exists() && f.isDirectory() )
				break;
		}				
		assertNotNull("Could not find directory " +
				"of test RDF, check in these locations:\n" +
				StringUtils.join(attempted,"\n  "), f);
		return f;
	}

	private static void indexRdf() throws Exception {
		RecordToDocument r2d = new RecordToDocumentMARC();
		
		for( String uri: rMarcURIS){
			SolrInputDocument doc;
			try {
				doc = r2d.buildDoc(uri, rdf);
				System.out.println(uri);
				System.out.println(doc.toString());
			} catch (Exception e) {
				System.out.println("failed on uri:" + uri);
				throw e;
			}
			try {
				solr.add( doc );
			} catch (Exception e) {
				System.out.println("Failed adding doc to solr for uri:" + uri);				
				System.out.println( IndexingUtilities.toString( doc ) + "\n\n" );
				System.out.println( IndexingUtilities.prettyFormat( ClientUtils.toXML( doc ) ) );
				throw e;
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
