package edu.cornell.library.integration.indexer;

import java.lang.reflect.Constructor;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparqlHttp;


/**
 * Class with a main method that will take a 
 * Endpoint URL and a document URI.
 *  
 * The output will be a solr document for that 
 * record written to stdout. 
 * 
 * A third argument for the URL of the solr index to
 * add the document to is optional.  
 */
public class CmdLineRecordToDocument {

	public static void main(String [] args){				
		if( help( args ) ) 
			return;
		
		String readEndpointURI = args[0];
		String recordURI = args[1];
		String recToDocImplClassName = args[2];				
		String solrIndexURL = (args.length > 3) ? args[3] : null;
		
		//make an instance of the RecordToDocument class
		RecordToDocument r2d = getRecordToDocumentImpl( recToDocImplClassName );		
		
		//setup SPARQL RDFService
		RDFService queryService = new RDFServiceSparqlHttp( readEndpointURI );
			
		//make the solr document
		try{			
			SolrInputDocument doc = r2d.buildDoc(recordURI, queryService);			
			System.out.print( doc.toString() );
			
			//index the solr doc if a server was specified 
			if( solrIndexURL != null )
				indexDoc( solrIndexURL, doc );
			
		}catch(Exception ex){
			System.err.println( ex.toString() );
			ex.printStackTrace(System.err);
			System.exit(1);
		}finally{
			queryService.close();
		}			
	}
	
	private static void indexDoc(String solrIndexURL, SolrInputDocument doc) throws Exception {
		SolrServer server = new CommonsHttpSolrServer(solrIndexURL);
		server.add( doc );
		server.commit();
		
		/*
Setting the RequestWriter

SolrJ lets you upload content in XML and Binary format. The default is set to be XML. Use the following to upload using Binary format. This is the same format which SolrJ uses to fetch results, and can greatly improve performance as it reduces XML marshalling overhead.
server.setRequestWriter(new BinaryRequestWriter());
Note -- be sure you have also enabled the "BinaryUpdateRequestHandler" in your solrconfig.xml for example like:

<requestHandler name="/update/javabin" class="solr.BinaryUpdateRequestHandler" />
		 */
	}

	private static RecordToDocument getRecordToDocumentImpl( String recToDocImplClassName){
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
	

	private static boolean help(String[] args) {		
		if( args == null || 
		    args.length < 3 ||
		    args.length > 4 ||
		    (args.length == 1 && args[0] != null && args[0].toLowerCase().startsWith("-h")) ){
			System.out.print(helptext);
			return true;
		}else{
			return false;
		}				
	}
	
	private static final String helptext =
		"Expected: URLofSPARQLEndpoint URLofRecordToIndex FQN_of_RecordToDocument_Class [URL_of_SOLR_index]\n";
}
