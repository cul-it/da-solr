package edu.cornell.library.integration.ILData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;

public class ILDataRemoteImpl implements ILData {
	Log log = LogFactory.getLog(ILDataRemoteImpl.class);
			
	private RDFService queryService;
			
	private List<String> holdingsIdQueries;
	private List<String> holdinigBibIdQueries;
	
	private List<String> bibQueries = Arrays.asList(
			
	"CONSTRUCT { $URI$ ?p ?o }WHERE{ $URI$ ?p ?o }",
	
	"prefix marc: <http://marcrdf.library.cornell.edu/canonical/0.1/> \n"+
	"prefix marcrdf: <http://marcrdf.library.cornell.edu/canonical/0.1/> \n"+
    "CONSTRUCT { \n"+
    "  $URI$ ?p ?f . \n"+
    "  ?p rdfs:subPropertyOf marcrdf:ControlFields. \n" +
    "  ?f ?fp ?fo. \n"+
    "  ?f marc:hasSubfield ?sf . \n"+
    "  ?sf ?sfp ?sfo. \n"+    
    " } WHERE { \n"+
    "  $URI$ ?p ?f. \n"+
    "  ?p rdfs:subPropertyOf marcrdf:DataFields. " +
    "  ?f ?fp ?fo. \n"+    
    "    optional{ \n"+
    "      ?f marc:hasSubfield ?sf \n"+
    "      optional{\n"+
    "         ?sf ?sfp ?sfo \n"+
    "      }\n"+
    "    }\n"+
    " }\n" );
	 
	
	public ILDataRemoteImpl( RDFService queryService ) throws IOException {		
		this.queryService = queryService;						
	}

	@Override
	public Model getBibData(String bibURI) {		
		try {
			return runQueries(bibURI, bibQueries);
		} catch (Exception e) {
			log.error("Could not get bib data for " + bibURI , e);
			return null;
		}		
	}

	@Override
	public Model getHoldingsData(String holdingsURI) {
		try {
			return runQueries(holdingsURI, holdingsIdQueries);
		} catch (Exception e) {
			log.error("Could not get holdings data for holding ID " + holdingsURI , e);
			return null;
		}		
	}

	@Override
	public Model getHoldingsDataForBib(String bibURI) {
		try {
			return runQueries(bibURI, holdinigBibIdQueries);
		} catch (Exception e) {
			log.error("Could not get holdings data for bib ID " + bibURI , e);
			return null;
		}		
	}

	private Model runQueries( String uri, List<String> queries ) throws RDFServiceException{		
		RDFService.ModelSerializationFormat format =  RDFService.ModelSerializationFormat.N3;
		String jenaSerializationFormat = "N3";
		
		Model mdl = ModelFactory.createDefaultModel();
		for( String query : makeQueries( uri , queries ) ){
			InputStream is = null;		
			is = queryService.sparqlConstructQuery(query ,format);
			mdl.read(is, null, jenaSerializationFormat ) ;
		} 
		return mdl;
	}
	
	private List<String> makeQueries( String uri, List<String> queries){
		List<String> out = new ArrayList<String>(queries.size());
		for( String q : queries){
			out.add( q.replace("$URI$", "<" + uri + ">"));
		}		
		return out;
	}
}
