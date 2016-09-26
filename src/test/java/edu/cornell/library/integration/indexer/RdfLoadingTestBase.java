package edu.cornell.library.integration.indexer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * Base class that can load RDF for tests.
 * @author bdc34
 *
 */
public class RdfLoadingTestBase {
	
	static RDFService rdf  = null;
	
	/**
	 * This test needs to load a set of RDF to use to build
	 * documents from. 
	 */
//	static final String testRDFDir =  "rdfToSolr/src/main/resources/";
	static final String testRDFDir =  "rdf/testRecords/";
	static final String standardRDFDir =  "rdfToSolr/build/resources/main/";
		
	
	static final String[] standardFiles = { "shadows.nt", "language_code.nt", "library.nt", "callnumber_map.nt", "fieldGroups.nt" };
	
    public static TemporaryFolder folder = null;
		
	public static void setupRdf() throws Exception{
		//load an RDF file of the radioactive MARC into a triple store 
		rdf = loadRdf();								
	}
			
	static RDFService loadRdf() throws Exception {		
		
		//load all files in test dir
		List<File> files = new LinkedList<>(Arrays.asList(new File("./"+testRDFDir).listFiles()));		
		assertNotNull("no test RDF files found",files);
		assertTrue("no test RDF files found", files.size() > 0 );
		
		//load the standard files
		for(String fileName : standardFiles){
			files.add( new File( new File("./"+standardRDFDir).getAbsolutePath() + 
                                 File.separator +fileName));
		}
		
		Model model = ModelFactory.createDefaultModel();
		for (File file : files ){
			try (InputStream in = FileManager.get().open( file.getAbsolutePath() )) {
				assertNotNull("Could not load file " + file.getAbsolutePath(), in );
				model.read(in,null,"N-TRIPLE");
			}
		}				
				
		return new RDFServiceModel( model );
	}
		
	@SuppressWarnings("static-method")
	public void testLanguageMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://da-rdf.library.cornell.edu/individual/leng>";
		assertTrue("Expected to find statements about English mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery(
                   "ASK WHERE { " + englishURI  
                   + " <http://da-rdf.library.cornell.edu/integrationLayer/0.1/code> ?a }"));
	}

	@SuppressWarnings("static-method")
	public void testCallnumberMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://da-rdf.library.cornell.edu/individual/lc_A>";
		assertTrue("Expected to find statements about lc callnumber prefix "+
                   "\"A\" mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery("ASK WHERE { " + englishURI + " ?p ?a }"));			
	}
}
