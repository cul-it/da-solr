package edu.cornell.library.integration.indexer;

import java.lang.reflect.Constructor;

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
		String recToDocImplClassName = args[2];
		
		//make an instance of the record 2 document class
		RecordToDocument r2d = getRecordToDocumentImpl( recToDocImplClassName );		
		
		//setup SPARQL RDFService
		RDFService queryService = new RDFServiceSparql( readEndpointURI );
					
		try{						
			SolrInputDocument doc = r2d.buildDoc(recordURI, queryService);
			System.out.print(doc.toString());
		}catch(Exception ex){
			System.err.print( ex.toString());
			ex.printStackTrace(System.err);
			System.exit(1);
		}finally{
			queryService.close();
		}			
	}
	
	private RecordToDocument getRecordToDocumentImpl( String recToDocImplClassName){
		try{
			Class recToDocImplClass = Class.forName(recToDocImplClassName);
			Constructor zeroArgCons = recToDocImplClass.getConstructor(null);
			return (RecordToDocument) zeroArgCons.newInstance(null);			
		}catch(Exception ex){
			System.err.println("could not instanciate class " + recToDocImplClassName);
			System.err.print( ex.toString());
			ex.printStackTrace(System.err);
			System.exit(1);
		}	
		return null;
	}
	

	private boolean help(String[] args) {		
		if( args == null || args.length != 3 ||
		    (args.length == 1 && args[0] != null && args[0].toLowerCase().startsWith("-h")) ){
			System.out.print(helptext);
			return true;
		}else{
			return false;
		}				
	}
	
	private static final String helptext ="Expected: URLofSPARQLEndpoint URLofRecordToIndex FQPN_of_RecordToDocument\n";
}
