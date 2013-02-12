package edu.cornell.library.integration.hadoop.map;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.fs.Path;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.io.Files;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.TDBLoader;
import com.hp.hpl.jena.tdb.store.GraphTDB;

import edu.cornell.library.integration.hadoop.BibFileToSolr;
import edu.cornell.library.integration.hadoop.HoldingForBib;
import edu.cornell.library.integration.hadoop.reduce.RdfToSolrIndexReducer;
import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This Mapper expects each item to have a URL of a bib or holdings N-Triples file.
 * For each <K,URL> this will generate zero or more <BIB_ID, StringOfNTriples> mappings.
 *   
 * @author bdc34
 *
 * @param <K> the incoming key is not used
 */
public class BibFileIndexingMapper <K> extends Mapper<K, Text, Text, Text>{
	Log log = LogFactory.getLog(BibFileIndexingMapper.class);
	
	
	String solrURL;
	SolrServer solr;
	
	//model for data that gets reused each reduce
	Model baseModel;
	
	HoldingForBib holdingsIndex;
	DavService davService;	

    Path todoDir;
    Path doneDir;

	public void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
        String url = urlText.toString();
        if( url == null || url.trim().length() == 0 ) 
            return; //skip blank lines

		File tmpDir = Files.createTempDir();
		log.info("Using tmpDir " + tmpDir.getAbsolutePath() + " for file based RDF store.");
		try{			
			InputStream is = getUrl( urlText.toString() , context );
			context.progress();
			
			log.info("Starting to build model");			
			//load the RDF to a triple store			
			Model model = TDBFactory.createModel(tmpDir.getAbsolutePath());
			TDBLoader loader = new TDBLoader() ;
			loader.loadGraph((GraphTDB)model.getGraph(), is);			
			model.add(baseModel);
			
			is.close(); is = null; //attempt do deallocate			
			context.progress();
			
			log.info("Model load completed. Starting query for all bib records in model. ");									
			Set<String> bibUris = getURIsInModel(context, model);
			context.progress();
			
			log.info("Getting holding rdf.");
			getHoldingRdf(context, bibUris, loader, model);		 
			
			log.info("Starting to index documents");
			RDFService rdf = new RDFServiceModel(model);
			for( String bibUri: bibUris){							
				indexToSolr(bibUri, rdf);	
				context.progress();
				context.getCounter(getClass().getName(), "bib uris indexed").increment(1);
			}		
			
			moveToDone( context );
		}catch(Throwable th){
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String filename = fileSplit.getPath().getName();
			String errorMsg = "could not process file URL " + urlText.toString() + " due to " + th.toString() ;
			context.write( new Text( filename), new Text( errorMsg ));
		}finally{
			Files.deleteRecursively(tmpDir);
		}
	}
	
	private void moveToDone( Context context ) throws java.io.IOException{
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		FileSystem fs = FileSystem.get( context.getConfiguration() );
		FileUtil.copy(fs, new Path(filename),fs, doneDir,
			true, false, fs.getConf()); 
	}

	private void indexToSolr(String bibUri, RDFService rdf){
		SolrInputDocument doc=null;
		try{
			RecordToDocument r2d = new RecordToDocumentMARC();
			doc = r2d.buildDoc(bibUri, rdf);
			if( doc == null ){
				log.error("No document created for " + bibUri);
				return;
			}
		}catch(Throwable er){
			log.error("Could not create solr document for " +bibUri 
					+ " " + er.getMessage());
			return;
		}
					
		try{
			solr.add(doc);				
		}catch (Throwable e) {			
			log.error("Could not add document to index for " + bibUri +
					" Check logs of solr server for details.");
		}
	}
	
	private void getHoldingRdf(Context context, Set<String> bibUris , TDBLoader loader , Model model){
		log.info("Getting additional holding data");
		Set<String> holdingUrls = new HashSet<String>();
		for( String bibUri : bibUris){
			try {
				holdingUrls.addAll( holdingsIndex.getHoldingUrlsForBibURI( bibUri ) );
			} catch (Exception e) {
				log.error("could not get holdings RDF for BibUri " + bibUri + 
						" " + e.getMessage());
			}
			
			context.progress();
		}			
		
		for( String holdingUrl : holdingUrls ){
			try {										
				loader.loadGraph((GraphTDB)model.getGraph(), getUrl(holdingUrl,context) );
				context.progress();
				context.getCounter(getClass().getName(), "holding urls loaded").increment(1);
			}catch (Exception e) {
				log.error("Could not get or load holding for URL for " +  holdingUrl 
						+ " " + e.getMessage());				
			}
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

	
	private Set<String> getURIsInModel( Context context, Model model) {
		Set<String>bibUris = new HashSet<String>();
		for( String queryStr : idQueries){
			try{
				Query query = QueryFactory.create(queryStr) ;
				QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
				try {
					ResultSet results = qexec.execSelect() ;
					for ( ; results.hasNext() ; ) {
				      QuerySolution soln = results.nextSolution() ;
				      Resource r = soln.getResource("URI") ; // Get a result variable - must be a resource
				      bibUris.add( r.getURI() );
				    }
				  } finally { qexec.close() ; }
			}catch(com.hp.hpl.jena.query.QueryParseException ex){
				log.error("Could not parse query for URIs, " + ex.getMessage() + " \n" + queryStr);
			}
		}		
		log.info("Number of URIs found: " + bibUris.size());
		return bibUris;
	}

    private String getSplitFileName(Context context){
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String filename = fileSplit.getPath().getName();
        return fileSplit.getPath().toString();
    }

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		Configuration conf = context.getConfiguration();
		
		todoDir = new Path( conf.get(BibFileToSolr.TODO_DIR) );
		doneDir = new Path( conf.get(BibFileToSolr.DONE_DIR) );
	
		solrURL = conf.get( RdfToSolrIndexReducer.SOLR_SERVICE_URL );
		if(solrURL == null )
			throw new Error("BibFileIndexingMapper requires URL of Solr server in config property " + RdfToSolrIndexReducer.SOLR_SERVICE_URL);
		
		solr = new CommonsHttpSolrServer(new URL(solrURL));
		try {
			solr.ping();
		} catch (SolrServerException e) {
			throw new Error("BibFileIndexingMapper cannot connect to solr server at \""+solrURL+"\".",e);
		}
		
		baseModel = loadBaseModel( context );
		
		holdingsIndex = new HoldingForBib("http://jaf30-dev.library.cornell.edu:8080/DataIndexer/showTriplesLocation.do");
		davService = new DavServiceImpl("admin","password");		
	}		
	
	private Model loadBaseModel(Context context) throws IOException {		
		Model baseModel = ModelFactory.createDefaultModel();		
		String[] baseNtFiles = { "/library.nt","/language_code.nt", "/callnumber_map.nt"};
		for( String fileName : baseNtFiles ){				
			InputStream in = getClass().getResourceAsStream(fileName);
			baseModel.read(in, null, "N-TRIPLE");
			in.close();
			log.info("loaded base model " + fileName);
		}		
		return baseModel;
	}		
	
	//queries to get URIs from model, expected to variable named URI in result set
	protected static final List<String> idQueries = Arrays.asList(
			//query for bib IDs from bib MARC
			"SELECT ?URI WHERE {\n" +
			"  ?URI " +
			"  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"  <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> " +
			"}"
//			//query for bib IDs from holdings MARC
//			,"SELECT ?URI WHERE {\n" +
//			"  ?holdingUri " +
//			"  <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> ?URI" +
//			" }" 						
	);				
		
//	private void runPerURIQueries(String uri, Context context, Model model) throws IOException, InterruptedException {
//		for(String constructTmp: perIdConstructs){
//			String constructStr = IndexingUtilities.substitueInRecordURI(uri, constructTmp);
//			try{							
//				QueryExecution qexec = QueryExecutionFactory.create(QueryFactory.create(constructStr), model) ;
//				try {
//					Model result = qexec.execConstruct();	
//				    context.write( new Text( uri ), new Text(MarcToSolrUtils.writeModelToNTString(result) ));
//				  } 
//				finally { qexec.close() ; }
//			}catch(com.hp.hpl.jena.query.QueryParseException ex){
//				log.error("Could not parse query for CONSTRUCT, " + ex.getMessage() + " \n" + constructStr);
//			}
//		}		
//	}

	//CONSTRUCT queries to run per URI from the idQueries. 
	//$recordURI$ will be replaced with a URI
//	protected static final List<String> perIdConstructs = Arrays.asList(			
//			//Get everything three levels deep from bib MARC
//			"CONSTRUCT { \n" +
//		    "  $recordURI$ ?p ?o .  ?o ?p2 ?o2 .  ?o2 ?p3 ?o4 . \n" + 				
//			"} WHERE { \n"+ 
//			"  $recordURI$ ?p ?o . \n" + 
//			"  optional{  ?o ?p2 ?o2 \n" +  //these OPTIONALS are nested 
//			"    optional { ?o2 ?p3 ?o4 } \n" +
//			"}}" ,
//			//Get everything three levels deep for holdings MARC
//			"CONSTRUCT { \n" +
//		    "  ?holdingURI ?p ?o .  ?o ?p2 ?o2 .  ?o2 ?p3 ?o4 . \n" +
//			"  ?holdingURI <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> $recordURI$ .\n" +
//			"} WHERE { \n"+
//		    "  ?holdingURI <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> $recordURI$ .\n" +
//			"  ?holdingURI ?p ?o . \n" + 
//			"  optional{  ?o ?p2 ?o2 \n" +  //these OPTIONALS are nested 
//			"    optional { ?o2 ?p3 ?o4 } \n" +
//			"}}"			
//			);	
}
