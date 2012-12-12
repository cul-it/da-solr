package edu.cornell.library.integration.hadoop.reduce;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.io.Files;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;

import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This reducer will take a key that is a URI of a bib and a list of
 * values that are strings of n-triple RDF and run a RDF to solr document
 * conversion on the RDF. Then the document is writteng to a solr service.
 */
public class RdfToSolrIndexReducer extends Reducer<Text, Text, Text, Text> {
	Log log = LogFactory.getLog(RdfToSolrIndexReducer.class);
	public final static String SOLR_SERVICE_URL = "integration.RdfToSolrReducer.SolrServiceUrl";
	public final static String SOLR_DOC_BATCH_SIZE = "integration.RdfToSolrReducer.SolrDocBatchSize";
    
	String solrURL;
	SolrServer solr;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);		
		Configuration conf = context.getConfiguration();
		
		solrURL = conf.get( SOLR_SERVICE_URL );
		if(solrURL == null )
			throw new Error("RdfToSolrReducer requires URL of Solr server in config property " + SOLR_SERVICE_URL);
		
		solr = new CommonsHttpSolrServer(new URL(solrURL));
		try {
			solr.ping();
		} catch (SolrServerException e) {
			throw new Error("RdfToSolrReducer cannot connect to solr server at \""+solrURL+"\".",e);
		}			
	}


	@Override	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {				
		
		File tmpDir = Files.createTempDir();
		Model model = null;
		try{				
			model = TDBFactory.createModel(tmpDir.getAbsolutePath());   			
			for( Text value : values){			
				try{
					Text.validateUTF8( value.getBytes() );
					model.read(new StringReader(value.toString()),null,"N-TRIPLE");
				}catch(Throwable ex){
					log.error( "could not load RDF for " + key.toString(),ex);
					log.error( "Problem with RDF:\n" + value.toString() );
					return;
				}
			}			
			
			SolrInputDocument doc=null;
			try{
				RecordToDocument r2d = new RecordToDocumentMARC();
				doc = r2d.buildDoc(key.toString(), new RDFServiceModel(model));
				if( doc == null )
					throw new Exception("No document created for " + key.toString());
			}catch(Throwable er){
				log.error("Could not create solr document for " + key.toString() , er);
				return;
			}
						
			try{				
				solr.add(doc);				
			}catch (Throwable e) {			
				log.error("Could not add document to index for " + key.toString() +
						" Check logs of solr server for details.");
			}
		}finally{
			trashModel( model, tmpDir );					
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		try {
			solr.commit();
		} catch (SolrServerException e) {
			throw new Error("Could not commit solr changes.",e);
		}
	}
	
	private void trashModel(Model model, File tmpDir) {		
		try{
			//need to reset TDB factory since it keeps static hidden 
			//state about datasets. (boo)
			TDBFactory.reset();			
		}catch(Throwable e){ 
			log.error( "could not reset TDBFactory: " + e.getMessage());
		}										
		try{ 
			Files.deleteDirectoryContents(tmpDir);			
		}catch(Throwable e){
			log.error("could not delete temp directory: " + e.getMessage());
		}
		try{ 
			Files.deleteRecursively(tmpDir);						
		}catch(Throwable e){
			log.error("could not delete temp directory: " + e.getMessage());
		}				
	}
}