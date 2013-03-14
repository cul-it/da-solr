package edu.cornell.library.integration.hadoop;

import static junit.framework.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ResourceF;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.tdb.TDBFactory;

import edu.cornell.library.integration.hadoop.helper.MarcToSolrUtils;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;


public class MarcToSolrUtilsTest {

	@Test
	public void getURLsTest() throws IOException{
		List<String> urls = MarcToSolrUtils.getLinksFromPage("http://culsearchdev.library.cornell.edu/data/voyager/bib/bib.nt.full/");
		assertNotNull( urls );		
		assertTrue( urls.size() > 0 );
	}
	
	

	//Unicode that should be in file 188_300000 for bib record 7519996
	String lookingFor ="广西壮族自治区";
			
			
	@Test
	public void utf8TestForWriteAndReadModelToNTString(){
		
		Model m = TDBFactory.createModel();
		m.add( ResourceFactory.createResource("http://example.com/bogus"), 
			   ResourceFactory.createProperty("http://example.com/bogusProp"),
			   ResourceFactory.createPlainLiteral(lookingFor) );
			
		assertTrue("expected to find "+ lookingFor + " in model" , utf8Found(m));
		
		String nt = MarcToSolrUtils.writeModelToNTString(m);
		Model m2 = MarcToSolrUtils.readNTStringToModel(nt);
		
		assertTrue("expected to find "+ lookingFor + " in re-created model" , utf8Found(m2));				
	}
	
	@Test
	public void utf8TestForHadoopText() throws CharacterCodingException{
		
		Model m = TDBFactory.createModel();
		m.add( ResourceFactory.createResource("http://example.com/bogus"), 
			   ResourceFactory.createProperty("http://example.com/bogusProp"),
			   ResourceFactory.createPlainLiteral(lookingFor) );
			
		assertTrue("expected to find "+ lookingFor + " in model" , utf8Found(m));
		
		String nt = MarcToSolrUtils.writeModelToNTString(m);
		assertTrue("expected to find " + lookingFor + " in model from Text() " ,
				utf8Found( 
				MarcToSolrUtils.readNTStringToModel(  
						(new Text(nt)).toString() )));
		
		ByteBuffer ba = Text.encode(nt);
		String nt2 = Text.decode( ba.array() );
		
		assertTrue("Expected to find " + lookingFor + " in model decoded by Text", 
				utf8Found( MarcToSolrUtils.readNTStringToModel( nt2 )));			
	}
	
	@Test 
	public void rdfServiceModelTest() throws RDFServiceException{
		Model m = TDBFactory.createModel();
		m.add( ResourceFactory.createResource("http://example.com/bogus"), 
			   ResourceFactory.createProperty("http://example.com/bogusProp"),
			   ResourceFactory.createPlainLiteral(lookingFor) );		
		assertTrue("expected to find "+ lookingFor + " in model" , utf8Found(m));
		
		RDFService rs = new RDFServiceModel(m);
		assertTrue("Expected to find " + lookingFor + " in RDFServiceModel", rs.sparqlAskQuery(q));
	}
	
	String q = "ASK WHERE{ ?s ?p ?o filter( contains( ?o , \""+ lookingFor + "\" ) ) } ";
	private boolean utf8Found(Model model ) {		
		QueryExecution qexec = QueryExecutionFactory.create(q, model);
		return  qexec.execAsk();		
	}

}
