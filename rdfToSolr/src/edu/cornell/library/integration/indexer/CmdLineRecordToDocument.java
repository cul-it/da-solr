package edu.cornell.library.integration.indexer;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;

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
public class CmdLineRecordToDocument extends CommandBase{

	public static void main(String [] args){				
		if( help( args ) ) 
			return;
		
		String readEndpointURI = args[0];
		String recordURI = args[1];
		String recToDocImplClassName = args[2];				
		String solrIndexURL = (args.length > 3) ? args[3] : null;
		
		//make an instance of the RecordToDocument class
		RecordToDocument r2d = getRecordToDocumentImpl( recToDocImplClassName );		
		r2d.setDebug(true);
		
		//setup SPARQL RDFService
		RDFService queryService = new RDFServiceSparqlHttp( readEndpointURI );
			
		//make the solr document
		try{			
			SolrInputDocument doc = r2d.buildDoc(recordURI, queryService);						
			System.out.println( toString( doc ) + "\n\n" );
			System.out.println( IndexingUtilities.prettyFormat( ClientUtils.toXML( doc ) ) );
			
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
	
	
	
	//TODO: maybe key of id field should be configurable
	private static final String idFieldKey = "id";

	private static void indexDoc(String solrIndexURL, SolrInputDocument doc) throws Exception {
		
		if( doc == null )
			throw new Exception("not attempting to index null SolrInputDocument");
		if( solrIndexURL == null )
			throw new Exception("solrIndexURL is required");
		if( doc.getField( idFieldKey ) == null )
			throw new Exception("a identifier field of '" +idFieldKey+"' is required");				    
		
        //It would be nice to use the default binary handler but there seem to be library problems
        CommonsHttpSolrServer server = 
        		new CommonsHttpSolrServer(new URL(solrIndexURL));
        		
		//SolrServer server = new CommonsHttpSolrServer(solrIndexURL);
		//server.setRequestWriter(new BinaryRequestWriter());
		
		//this might not work well if there are multiple values for the id field        
        String idValue = ClientUtils.escapeQueryChars( (String)doc.getField( idFieldKey ).getFirstValue() );         
		server.deleteByQuery(idFieldKey + ":" + idValue );
		
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		docs.add( doc );
		
		UpdateResponse resp = server.add(docs);
		System.out.println( respToString( resp ) );
		
		server.commit();		
	}
	
	private static String respToString( UpdateResponse resp){
		String out="response not yet set";
		if( resp == null ){
			out = "no responnse";
		}else{
			int status = resp.getStatus();
			NamedList<Object>objs = resp.getResponse();
			out = "status: " +status + "\n";
			if( objs != null ){
				Iterator<Entry<String,Object>> it =  objs.iterator();
				while(it.hasNext()){
					Entry<String, Object> e = it.next();
					out = out + " " + e.getKey() + ": " + e.getValue() + "\n";
				}				
			}
		}
		return out;
	}
}
