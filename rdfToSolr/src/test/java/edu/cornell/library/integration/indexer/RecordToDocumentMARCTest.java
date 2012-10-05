package edu.cornell.library.integration.indexer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

public class RecordToDocumentMARCTest {
	SolrServer solr = null;	
	RDFService rdf  = null;
	
	final String fallbackSolrDir1 = new File("../solr").getAbsolutePath() ;
	final String fallbackSolrDir2 = new File("./solr").getAbsolutePath() ;
	final String fallbackMARC1 = new File("../rdf/output/RadMARCATS1.nt").getAbsolutePath() ;
	final String fallbackMARC2 = new File("./rdf/output/RadMARCATS1.nt").getAbsolutePath() ;

	final String[] rMarcURIS = {
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
	
	@Rule
    public TemporaryFolder folder = new TemporaryFolder();
	
	@Before
	public void setup() throws Exception{
		
		String solrTemplateDir = getSolrTemplateDir();
		if( solrTemplateDir == null)
			throw new Exception("could not find solr template directory");					
		
		File solrBase = prepareTmpSolrDir( solrTemplateDir, folder );
		solr = setupSolrIndex( solrBase );		
		indexStandardTestRecords( solr );		
	}

	@Test
	public void testSolrInit() throws SolrServerException, IOException {
		assertNotNull( solr );
		solr.ping();
	}
	
	
	/**
	 * First attempt: get the value from a resource file.
	 * (didn't work out)
	 * Second attempt: guess based on CWD
	 * @return
	 */
	private String getSolrTemplateDir() {
		String key = "solrTemplateDirectory";
		System.out.println( new File(".").getAbsolutePath() );
		//get the testSolr.properties file and load it
		URL url = getResource("/testSolr.properties");
		Properties props = new Properties();
		if (null != url) {
            try {
                InputStream in = url.openStream();
                props = new Properties();
                props.load(in);
            } catch (IOException e) {
                //throw new Exception("cannot load testSolr.properties resource", e);
            }
		}
//		else{
//			throw new Exception("cannot load testSolr.properties resource, " +
//					"make sure to run prepareForTesting task");
//		}
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

	public File prepareTmpSolrDir(String solrTemplateDir, TemporaryFolder folder) throws IOException{
		File base = folder.newFolder("recordToDocumentMARCTest_solr");
		System.out.println( solrTemplateDir );
		FileUtils.copyDirectory(new File(solrTemplateDir), base );				
		return base;
	}
	
	public SolrServer setupSolrIndex(File solrBase) throws ParserConfigurationException, IOException, SAXException{
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
		
	private void indexStandardTestRecords(SolrServer solr2) throws RDFServiceException, Exception {
		//load an RDF file of the radioactive MARC into a triple store 
		rdf = loadRMARC();
		
		//and index solr documents for them.  
		indexRdf();
	}

	private RDFService loadRMARC() throws Exception {
		
		Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open( fallbackMARC1 );
		if (in == null) {
		   in = FileManager.get().open( fallbackMARC2 );
		   if( in == null ){
			   throw new Exception("Could not load marc file " + fallbackMARC2);
		   }
		}
		model.read(in,null,"N-TRIPLE"); 
		return new RDFServiceModel( model );
	}
	
	private void indexRdf() throws Exception {
		RecordToDocument r2d = new RecordToDocumentMARC();
		
		for( String uri: rMarcURIS){
			SolrInputDocument doc;
			try {
				doc = r2d.buildDoc(uri, rdf);
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
	public URL getResource(String resource){
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
	    classLoader = getClass().getClassLoader();
	    if(classLoader != null){
	        url = classLoader.getResource(resource);
	        if(url != null){
	            return url;
	        }
	    }

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
