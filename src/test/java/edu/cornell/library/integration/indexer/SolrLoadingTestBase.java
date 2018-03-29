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
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.rules.TemporaryFolder;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.utilities.IndexingUtilities;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

public class SolrLoadingTestBase extends RdfLoadingTestBase {
			
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
	
	static final String[] rMarcURIS = {
			"http://da-rdf.library.cornell.edu/individual/b3309",
			"http://da-rdf.library.cornell.edu/individual/b4696",
			"http://da-rdf.library.cornell.edu/individual/b1322952",
			"http://da-rdf.library.cornell.edu/individual/b7683714",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC001",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC002",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC003",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC004",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC005",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC006",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC007",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC008",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC009",
			"http://da-rdf.library.cornell.edu/individual/bUNTRadMARC010"
	};
	
	
    public static TemporaryFolder solrTmpFolder = null;
		
	public static void setupSolr() throws Exception{		
		setupRdf();
		
		solrTmpFolder = new TemporaryFolder();
		solrTmpFolder.create();
		
		String solrTemplateDir = getSolrTemplateDir();
		if( solrTemplateDir == null)
			throw new Exception("could not find solr template directory");					
		
		File solrBase = prepareTmpSolrDir( solrTemplateDir, solrTmpFolder );
		solr = setupSolrIndex( solrBase );		
		indexStandardTestRecords( solr, rdf );		
	}
	
	public static void takeDownSolr(){		
		solrTmpFolder.delete();		
	}
	
	public static void testSolrWasStarted() throws SolrServerException, IOException {
		assertNotNull( solr );
		solr.ping();
	}
	
	public static void testRadioactiveIds() throws SolrServerException, IOException{	
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
	static void testQueryGetsDocs(String errmsg, SolrQuery query, String[] docIds) throws SolrServerException, IOException{
		assertNotNull(errmsg + " but query was null", query);
		assertNotNull(errmsg + " but docIds was null", docIds );
									
		QueryResponse resp = solr.query(query);
		if( resp == null )
			fail( errmsg + " but Could not get a solr response");
		
		Set<String> expecteIds = new HashSet<>(Arrays.asList( docIds ));
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
            try ( InputStream in = url.openStream() ) {
            	props.load(in);
            }
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

	private static File prepareTmpSolrDir(String solrTemplateDir, TemporaryFolder folder) throws IOException{
		File base = folder.newFolder("recordToDocumentMARCTest_solr");
		System.out.println( solrTemplateDir );
		FileUtils.copyDirectory(new File(solrTemplateDir), base );				
		return base;
	}

	private static SolrClient setupSolrIndex(File solrBase) {//throws ParserConfigurationException, IOException, SAXException{
//		System.setProperty("solr.solr.home", solrBase.getAbsolutePath());
//		CoreContainer.Initializer initializer = new CoreContainer.Initializer();
//		CoreContainer coreContainer = initializer.initialize();
//		return new EmbeddedSolrServer(coreContainer, "");
	    //bdc34: some how I no longer have the EmbeddedSolrServer class
	    //I think that this might have been removed from solr 4.x
	    return null;
	}


	private static void indexStandardTestRecords( SolrClient solr , RDFService rdfService) throws Exception {
		RecordToDocument r2d = new RecordToDocumentMARC();
		
		Config config = Config.loadConfig( new String[2] );
		
		for( String uri: rMarcURIS){
			SolrInputDocument doc;
			try {
				doc = r2d.buildDoc(uri, config);
			} catch (Exception e) {
				System.out.println("failed on uri:" + uri);
				throw e;
			}
			try {
				solr.add( doc );
			} catch (Exception e) {
				System.out.println("Failed adding doc to solr for uri:" + uri);				
				System.out.println( IndexingUtilities.toString( doc ) + "\n\n" );
				System.out.println( IndexingUtilities.prettyXMLFormat( ClientUtils.toXML( doc ) ) );
				throw e;
			}
		}
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
