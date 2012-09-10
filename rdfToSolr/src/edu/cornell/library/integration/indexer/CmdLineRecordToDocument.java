package edu.cornell.library.integration.indexer;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparql;


/**
 * Class with a main method that will take a 
 * Endpoint URL and a document URI.
 *  
 * The output will be a solr document for that 
 * record written to stdout. 
 */
public class CmdLineRecordToDocument {

	public void main(String [] args){				
		if( help( args ) ) 
			return;
		
		String readEndpointURI = args[0];
		String recordURI = args[1];
		
		//setup SPARQL RDFService
		RDFService queryService = new RDFServiceSparql( readEndpointURI );
	
		SolrInputDocument doc = null;
		Exception error = null;		
		try{
			//call RecordToDocument
			doc = (new RecordToDocumentImpl()).buildDoc(recordURI, queryService);
		}catch(Exception ex){
			error = ex;
		}finally{
			queryService.close();
		}
				
		//output solr document
		if( doc != null)
			System.out.print(doc.toString());
		if( error != null ){
			System.err.print( error.toString());
			error.printStackTrace(System.err);
		}
	}

	private boolean help(String[] args) {		
		if( args == null || args.length != 2 ||
		    (args.length == 1 && args[0] != null && args[0].toLowerCase().startsWith("-h")) ){
			System.out.print(helptext);
			return true;
		}else{
			return false;
		}				
	}
	
	private static final String helptext ="Expected: URLofSPARQLEndpoint URLofRecordToIndex\n";
}
