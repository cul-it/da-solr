package edu.cornell.library.integration.hadoop.reduce;

import java.io.IOException;
import java.io.InputStream;
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

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This reducer will take a key that is a URI of a bib and a list of
 * values that are strings of n-triple RDF and run a RDF to solr document
 * conversion on the RDF. Then the document is written to a solr service.
 */
public class RdfToSolrIndexReducer extends Reducer<Text, Text, Text, Text> {
	Log log = LogFactory.getLog(RdfToSolrIndexReducer.class);
	public final static String SOLR_SERVICE_URL = "integration.RdfToSolrReducer.SolrServiceUrl";
	public final static String SOLR_DOC_BATCH_SIZE = "integration.RdfToSolrReducer.SolrDocBatchSize";
    	
	String solrURL;
	SolrServer solr;
	
	//model for data that gets reused each reduce
	Model baseModel;
	
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
		
		baseModel = loadBaseModel( context );
	}

	@Override	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {						
		
		//read RDF from values into model
		Model perReduceModel = ModelFactory.createDefaultModel();			
		for( Text value : values){			
			try{
				Text.validateUTF8( value.getBytes() );
				perReduceModel.read(new StringReader(value.toString()),null,"N-TRIPLE");
			}catch(Throwable ex){
				log.error( "could not load RDF for " + key.toString(),ex);
				log.error( "Problem with RDF:\n" + value.toString() );
				return;
			}
		}			
				
		//make union model with base and per reducer models
		Model model = ModelFactory.createUnion(perReduceModel, baseModel);						
		
		SolrInputDocument doc=null;
		try{
			RecordToDocument r2d = new RecordToDocumentMARC();
			doc = r2d.buildDoc(key.toString(), new RDFServiceModel(model));
			if( doc == null ){
				log.error("No document created for " + key.toString());
				return;
			}
		}catch(Throwable er){
			log.error("Could not create solr document for " + key.toString() 
					+ " " + er.getMessage());
			return;
		}
					
		try{				
			solr.add(doc);				
		}catch (Throwable e) {			
			log.error("Could not add document to index for " + key.toString() +
					" Check logs of solr server for details.");
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
	
	private Model loadBaseModel(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException {		
		Model baseModel = ModelFactory.createDefaultModel();		
		String[] baseNtFiles = { "/library.nt","/language_code.nt"};
		for( String fileName : baseNtFiles ){				
			InputStream in = getClass().getResourceAsStream(fileName);
			baseModel.read(in, null, "N-TRIPLE");
			in.close();
			log.info("loaded base model " + fileName);
		}		
		return baseModel;
	}		
}