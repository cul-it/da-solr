package edu.cornell.library.integration.hadoop.map;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
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
import edu.cornell.library.integration.hadoop.helper.HoldingForBib;
import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This Mapper expects each item to have a URL of a bib or holdings N-Triples file.
 * For each <K,URL> this will load the RDF from the URL and index a Solr document 
 * each bib record found in the RDF.
 *   
 * @author bdc34
 *
 * @param <K> the incoming key is not used
 */
public class BibFileIndexingMapper <K> extends Mapper<K, Text, Text, Text>{
	Log log = LogFactory.getLog(BibFileIndexingMapper.class);
	
    //hadoop directory for the input splits that are completed 
    Path doneDir;
	
	String solrURL;
	SolrServer solr;
	
	//model for data that gets reused each reduce
	Model baseModel;
	
	HoldingForBib holdingsIndex;
	DavService davService;		

	//true of an error happened during a single call to map()
	boolean errors_encountered = false;
	
	public void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
		errors_encountered = false;
		
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
			
			is.close();			
			context.progress();
			
			log.info("Model load completed. Starting query for all bib records in model. ");									
			Set<String> bibUris = getURIsInModel(context, model);
			context.progress();
			
			log.info("Getting holding rdf.");
			getHoldingRdf(context, bibUris, loader, model);		 
			
			log.info("Starting to index documents");
			RDFService rdf = new RDFServiceModel(model);
			for( String bibUri: bibUris){	
				try{
					indexToSolr(bibUri, rdf);	
					context.progress();
					context.getCounter(getClass().getName(), "bib uris indexed").increment(1);
					context.write(new Text(bibUri), new Text("URI\tSuccess"));
				}catch(Throwable ex ){
					String filename = getSplitFileName(context);
					context.write(new Text(bibUri), new Text("URI\tError\t"+ex.getMessage()));
				}
			}		
						
			moveToDone( context );
		}catch(Throwable th){			
			String filename = getSplitFileName(context);
			String errorMsg = "could not process file URL " + urlText.toString() +
					" due to " + th.toString() ;
			log.error( errorMsg );
			context.write( new Text( filename), new Text( "FILE\tError\t"+errorMsg ));
		}finally{			
			FileUtils.deleteDirectory( tmpDir );			
		}		
	}
	
	/** Move the split from the todo directory to the done directory. */ 
	private void moveToDone( Context context ) throws java.io.IOException{		
		String filename = getSplitFileName(context);
		FileSystem fs = FileSystem.get( context.getConfiguration() );
		FileUtil.copy(fs, new Path(filename),fs, doneDir,
			true, false, fs.getConf()); 
	}

	private void indexToSolr(String bibUri, RDFService rdf) throws Exception{
		SolrInputDocument doc=null;
		try{
			RecordToDocument r2d = new RecordToDocumentMARC();
			doc = r2d.buildDoc(bibUri, rdf);
			if( doc == null ){
				throw new Exception("No document created for " + bibUri);				
			}
		}catch(Throwable er){			
			throw new Exception ("Could not create solr document for " +bibUri, er);			
		}
					
		try{
			solr.add(doc);				
		}catch (Throwable er) {			
			throw new Exception("Could not add document to index for " + bibUri +
					" Check logs of solr server for details.", er );
		}
	}
	
	/** Get the holding RDF for all the bib records inn bibUris and add them to model. */
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

	/** Attempt to get all the bib record URIs from model. */
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
				      Resource r = soln.getResource("URI") ; //result variable must be a resource
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

	/** get the filename for the current file (aka input split) that is being worked on. */
    private String getSplitFileName(Context context){
    	
    	FileSplit fileSplit = (FileSplit)context.getInputSplit();
		return fileSplit.getPath().getName();			
    }

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		Configuration conf = context.getConfiguration();
				
		doneDir = new Path( conf.get(BibFileToSolr.DONE_DIR) );
		if(doneDir == null )
			throw new Error("Requires directory of HDFS for the done directory in configuration, " +
					"this should have been set by BibFileToSolr or the parent hadoop job. " 
					+ BibFileToSolr.DONE_DIR);
		
		solrURL = conf.get( BibFileToSolr.SOLR_SERVICE_URL );
		if(solrURL == null )
			throw new Error("Requires URL of Solr server in config property " 
					+ BibFileToSolr.SOLR_SERVICE_URL);
				
		solr = new ConcurrentUpdateSolrServer(solrURL, 100, 2);
		
		try { solr.ping(); } catch (SolrServerException e) {
			throw new Error("Cannot connect to solr server at \""+solrURL+"\".",e);
		}
		
		baseModel = loadBaseModel( context );
				
		holdingsIndex = new HoldingForBib(
				conf.get(BibFileToSolr.HOLDING_SERVICE_URL));
		
		davService = new DavServiceImpl(
				conf.get(BibFileToSolr.BIB_WEBDAV_USER),
				conf.get(BibFileToSolr.BIB_WEBDAV_PASSWORD));		
	}		
	
	private Model loadBaseModel(Context context) throws IOException {		
		Model baseModel = ModelFactory.createDefaultModel();		
		String[] baseNtFiles = { "/shadows.nt","/library.nt","/language_code.nt", "/callnumber_map.nt","/fieldGroups.nt"};
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
	
}
