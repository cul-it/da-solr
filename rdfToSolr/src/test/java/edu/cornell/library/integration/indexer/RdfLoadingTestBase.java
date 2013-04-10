package edu.cornell.library.integration.indexer;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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
	
	
	static final String[] standardFiles = { "language_code.nt", "library.nt", "callnumber_map.nt", "fieldGroups.nt" };
	
    public static TemporaryFolder folder = null;
		
	public static void setupRdf() throws Exception{
		//load an RDF file of the radioactive MARC into a triple store 
		rdf = loadRdf();								
	}
			
	static RDFService loadRdf() throws Exception {		
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
		
	public void testLanguageMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://fbw4-dev.library.cornell.edu/individuals/leng>";
		assertTrue("Expected to find statements about English mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery("ASK WHERE { " + englishURI + " <http://fbw4-dev.library.cornell.edu/integrationLayer/0.1/code> ?a }"));			
	}
			
	public void testCallnumberMappingsInRDF() throws RDFServiceException{
		String englishURI = "<http://fbw4-dev.library.cornell.edu/individuals/lc_A>";
		assertTrue("Expected to find statements about lc callnumber prefix \"A\" mappings in the RDF. " +
				"The mappings RDF may not be getting loaded for this test.",
				rdf.sparqlAskQuery("ASK WHERE { " + englishURI + " ?p ?a }"));			
	}

	
	protected static File findTestDir() {
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
}
