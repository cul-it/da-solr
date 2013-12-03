package edu.cornell.library.integration.indexer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.sql.Connection;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.indexer.utilies.IndexingUtilities;
import edu.cornell.library.integration.support.OracleQuery;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparqlHttp;

/**
 * Index records from stdin, one URL per a line.
 * 
 */
public class StdinRecordToDocument extends CommandBase {
	static String idFieldKey = "id";
	static int commitCount = 1;
	
	public static void main(String [] args){				
		if( help( args ) ) 
			return;
				
		String rdfEndpointURI = args[0];		
		String recToDocImplClassName = args[1];				
		String solrIndexURL = args[2];
		
		//make an instance of the RecordToDocument class
		RecordToDocument r2d = getRecordToDocumentImpl( recToDocImplClassName );		
		r2d.setDebug(true);
		
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
			int count =0;
			BufferedReader in = new BufferedReader(new InputStreamReader( System.in ));
			Connection voyager = OracleQuery.openConnection(OracleQuery.DBDriver, OracleQuery.DBProtocol, 
					OracleQuery.DBServer, OracleQuery.DBName, OracleQuery.DBuser, OracleQuery.DBpass);
			String recordURI = in.readLine();
			while(  recordURI != null ){
				
				System.err.println(recordURI);
				try{
					addToIndex( solrServer, r2d.buildDoc(recordURI, rdfService, voyager) );
				}catch(Exception ex){
					System.out.println("exception while working on " + recordURI + "\n" + ex.getMessage());
					ex.printStackTrace(System.out);
				}
				count++;
				if( count % commitCount == 0)
					solrServer.commit();
				
				recordURI = in.readLine();
			}
			OracleQuery.closeConnection(voyager);
			
			solrServer.commit();
		}catch(Exception ex){
			System.out.println( ex.toString() );
			ex.printStackTrace(System.out);
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
		
		System.out.println( IndexingUtilities.toString(doc) );
		solrServer.add(doc);			
	}

	static SolrServer setupSolrServer( String endPointURI ) throws MalformedURLException{
        return new HttpSolrServer(endPointURI);        		
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
