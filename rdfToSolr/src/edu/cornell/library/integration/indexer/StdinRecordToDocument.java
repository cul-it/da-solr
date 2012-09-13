package edu.cornell.library.integration.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparqlHttp;

/**
 * Index records from stdin, one URL per a line.
 * 
 */
public class StdinRecordToDocument extends CommandBase {
	static String idFieldKey = "id";
	
	public static void main(String [] args){				
		if( help( args ) ) 
			return;
				
		String rdfEndpointURI = args[0];		
		String recToDocImplClassName = args[1];				
		String solrIndexURL = args[2];
		
		//make an instance of the RecordToDocument class
		RecordToDocument r2d = getRecordToDocumentImpl( recToDocImplClassName );		
		
		//setup SPARQL RDFService
		RDFService rdfService = new RDFServiceSparqlHttp( rdfEndpointURI );
			
		//setup solr server
		SolrServer solrServer = null;
		try {
			solrServer = setupSolrServer(solrIndexURL);
		} catch (MalformedURLException e) {
			System.err.println("could not setup connection with solr index: " + e.getMessage());
			System.exit(1);
		}
		
		try{	
			BufferedReader in = new BufferedReader(new InputStreamReader( System.in ));
			String recordURI = in.readLine();
			while(  recordURI != null ){
				System.err.println(recordURI);
				addToIndex( solrServer, r2d.buildDoc(recordURI, rdfService) );
				recordURI = in.readLine();
			}
			
			solrServer.commit();
		}catch(Exception ex){
			System.err.println( ex.toString() );
			ex.printStackTrace(System.err);
			System.exit(1);
		}finally{			
			rdfService.close();			
		}			
	}

	
	private static void addToIndex(SolrServer solrServer, SolrInputDocument doc) 
			throws SolrServerException, IOException {

		//this might not work well if there are multiple values for the id field        
        String idValue = ClientUtils.escapeQueryChars( (String)doc.getField( idFieldKey ).getFirstValue() );         
		solrServer.deleteByQuery(idFieldKey + ":" + idValue );				
		solrServer.add(doc);			
	}

	static SolrServer setupSolrServer( String endPointURI ) throws MalformedURLException{
        return  new CommonsHttpSolrServer(new URL(endPointURI));        		
	}
	
	private static String help = "\nexpected: endPointURI recToDocImplClassName solrIndexURL\n";
	private static boolean help(String[] args) {
		if( args == null || args.length != 3 ){
			System.err.print(help);
			return true;
		}else{
			return false;
		}
	}
	

}
