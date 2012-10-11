package edu.cornell.library.integration.recordextract;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import edu.cornell.library.integration.indexer.IndexingUtilities;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.sparql.RDFServiceSparqlHttp;

/**
 * Run the construct queries and return the results as N-TRIPLES
 * 
 */
public class RecordExtract {
	static final String[] queryFiles = {
		"query1.txt" 
		,"query2.txt"		
		,"query3.txt"
		};
	
	public static void main(String [] args){				
		
		String readEndpointURL = args[0];
		String recordURI = args[1];			
		
		//setup SPARQL RDFService
		RDFService queryService = new RDFServiceSparqlHttp( readEndpointURL );
			
		try{			
			runConstruct( queryService, recordURI, System.out);			
		}catch(Exception ex){
			System.err.println( ex.toString() );
			ex.printStackTrace(System.err);
			System.exit(1);
		}finally{
			queryService.close();
		}			
	}

	/** 
	 * Run a SPARQL CONSTRUCT for a record and output to out.
	 * 
	 * @param queryService
	 * @param recordURI
	 * @param out 
	 * @throws Exception 
	 */
	private static void runConstruct(RDFService queryService, String recordURI, PrintStream out) throws Exception {
		RDFService.ModelSerializationFormat format =  RDFService.ModelSerializationFormat.NTRIPLE;
		 
		for( String query : getQueries(recordURI) ){
			InputStream is = null;		
			is = queryService.sparqlConstructQuery(query ,format);
			IOUtils.copy(is, out);			
		}
	}

	private static List<String> getQueries(String recordURI) throws Exception {
		ArrayList<String> queries = new ArrayList<String>();
		for( String fname : queryFiles ){
			InputStream is = getForQuery( fname );
			if( is == null ) 
				throw new Exception("Cannot find resource " + fname);
			String q = IOUtils.toString(is,"UTF-8");
			queries.add(IndexingUtilities.substitueInRecordURI(recordURI, q));
		}
		return queries;
	}

	@SuppressWarnings("static-access")
	private static InputStream getForQuery(String fname) {
		String p = "edu/cornell/library/integration/recordextract/queries/";			
		return  RecordExtract.class.getClassLoader().getSystemResourceAsStream( p + fname);		
	}

	

}
