package edu.cornell.library.integration.hadoop.map;

import static org.openjena.riot.Lang.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openjena.riot.RiotReader;

import com.google.common.io.Files;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.store.GraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.store.bulkloader.Destination;

import edu.cornell.library.integration.hadoop.MarcToSolrUtils;
import edu.cornell.library.integration.indexer.IndexingUtilities;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;

/**
 * This Mapper expects each item to have a URL of a bib or holdings N-Triples file.
 * For each <K,URL> this will generate zero or more <BIB_ID, StringOfNTriples> mappings.
 *   
 * @author bdc34
 *
 * @param <K> the incoming key is not used
 */
public class URLToMarcRdfFetchMapper <K> extends Mapper<K, Text, Text, Text>{
	Log log = LogFactory.getLog(URLToMarcRdfFetchMapper.class);
	
	File tmpDir;
	DavService davService;	
	
	public void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
		InputStream is = getUrl( urlText.toString() , context );
												
		//load the RDF to a triple store
		Files.deleteDirectoryContents(tmpDir);		
		Model model = TDBFactory.createModel(tmpDir.getAbsolutePath());
		TDBLoader loader = new TDBLoader() ;
		loader.loadGraph((GraphTDB)model.getGraph(), is);				
		
		//BulkLoader.loadDefaultGraph(((GraphTDB)model.getGraph()).getDataset(), is, false);
		
	        
		//find all records in triple store
		for( String uri: getURIsInModel(context, model)){							
			runPerURIQueries( uri, context, model );
		}					
	}
	
	private InputStream getUrl(String url, Context context) throws IOException {
		InputStream is = null;
		try {
			is = davService.getFileAsInputStream(url);
			if( url.endsWith( ".gz" ) || url.endsWith(".gzip"))
				return new GZIPInputStream( is );
			else
				return is;
		} catch (Exception e) {
			throw new IOException("Could not get " + url , e);			
		}		
	}

	private void runPerURIQueries(String uri, Context context, Model model) throws IOException, InterruptedException {
		for(String constructTmp: perIdConstructs){
			String constructStr = IndexingUtilities.substitueInRecordURI(uri, constructTmp);
			try{							
				QueryExecution qexec = QueryExecutionFactory.create(QueryFactory.create(constructStr), model) ;
				try {
					Model result = qexec.execConstruct();	
				    context.write( new Text( uri ), new Text(MarcToSolrUtils.writeModelToNTString(result) ));
				  } 
				finally { qexec.close() ; }
			}catch(com.hp.hpl.jena.query.QueryParseException ex){
				log.error("Could not parse query for CONSTRUCT, " + ex.getMessage() + " \n" + constructStr);
			}
		}		
	}

	private Set<String> getURIsInModel( Context context, Model model) {
		Set<String>ids = new HashSet<String>();
		for( String queryStr : idQueries){
			try{
				Query query = QueryFactory.create(queryStr) ;
				QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
				try {
					ResultSet results = qexec.execSelect() ;
					for ( ; results.hasNext() ; ) {
				      QuerySolution soln = results.nextSolution() ;
				      Resource r = soln.getResource("URI") ; // Get a result variable - must be a resource
				      ids.add( r.getURI() );
				    }
				  } finally { qexec.close() ; }
			}catch(com.hp.hpl.jena.query.QueryParseException ex){
				log.error("Could not parse query for URIs, " + ex.getMessage() + " \n" + queryStr);
			}
		}
		log.info("Number of URIs found: " + ids.size());
		return ids;
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		davService = new DavServiceImpl("admin","password");
		tmpDir = Files.createTempDir();
		log.debug("Using tmpDir " + tmpDir.getAbsolutePath());
	}
	
	@Override
	public void cleanup(Context context)throws IOException, InterruptedException{
		Files.deleteRecursively( tmpDir );
	}
	
	//queries to get URIs from model, expected to variable named URI in result set
	protected static final List<String> idQueries = Arrays.asList(
			//query for bib IDs from bib MARC
			"SELECT ?URI WHERE {\n" +
			"  ?URI " +
			"  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"  <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> " +
			"}",
			//query for bib IDs from holdings MARC
			"SELECT ?URI WHERE {\n" +
			"  ?holdingUri " +
			"  <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> ?URI" +
			" }" 						
	);				
		
	//CONSTRUCT queries to run per URI from the idQueries. 
	//$recordURI$ will be replaced with a URI
	protected static final List<String> perIdConstructs = Arrays.asList(			
			//Get everything three levels deep from bib MARC
			"CONSTRUCT { \n" +
		    "  $recordURI$ ?p ?o .  ?o ?p2 ?o2 .  ?o2 ?p3 ?o4 . \n" + 				
			"} WHERE { \n"+ 
			"  $recordURI$ ?p ?o . \n" + 
			"  optional{  ?o ?p2 ?o2 \n" +  //these OPTIONALS are nested 
			"    optional { ?o2 ?p3 ?o4 } \n" +
			"}}" ,
			//Get everything three levels deep for holdings MARC
			"CONSTRUCT { \n" +
		    "  ?holdingURI ?p ?o .  ?o ?p2 ?o2 .  ?o2 ?p3 ?o4 . \n" +
			"  ?holdingURI <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> $recordURI$ .\n" +
			"} WHERE { \n"+
		    "  ?holdingURI <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> $recordURI$ .\n" +
			"  ?holdingURI ?p ?o . \n" + 
			"  optional{  ?o ?p2 ?o2 \n" +  //these OPTIONALS are nested 
			"    optional { ?o2 ?p3 ?o4 } \n" +
			"}}"			
			);	
}
