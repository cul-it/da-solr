package edu.cornell.library.integration.indexer;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

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
			System.out.println( toString( doc ) );
			System.out.println( ClientUtils.toXML( doc ) );
			
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
	

	//TODO: maybe key of id field should be configurable
	private static final String idFieldKey = "id";

	private static void indexDoc(String solrIndexURL, SolrInputDocument doc) throws Exception {
		
		if( doc == null )
			throw new Exception("not attempting to index null SolrInputDocument");
		if( solrIndexURL == null )
			throw new Exception("solrIndexURL is required");
		if( doc.getField( idFieldKey ) == null )
			throw new Exception("a identifier field of '" +idFieldKey+"' is required");				    
		
        boolean useMultiPartPost = true;
        //It would be nice to use the default binary handler but there seem to be library problems
        CommonsHttpSolrServer server = 
        		//new CommonsHttpSolrServer(new URL(solrIndexURL),null,new XMLResponseParser(),useMultiPartPost);
        		new CommonsHttpSolrServer(new URL(solrIndexURL));
        		
		//SolrServer server = new CommonsHttpSolrServer(solrIndexURL);
		//server.setRequestWriter(new BinaryRequestWriter());
		
		//this might not work well if there are multiple values for the id field
		server.deleteByQuery(idFieldKey + ":" + doc.getField( idFieldKey ).getValue());
		
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		docs.add( doc );
		
		server.add(docs);		
		server.commit();		
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
		"Expected: URLofSPARQLEndpoint URLofRecordToIndex FQN_of_RecordToDocument_Class [solrIndexURL]\n";
	
	private static String toString(SolrInputDocument doc){
		String out ="SolrInputDocument[\n" ;
		for( String name : doc.getFieldNames()){
			SolrInputField f = doc.getField(name);
			out = out + "  " + name +": '" + f.toString() + "'\n";
		}
		return out + "]\n";						
	}
}
