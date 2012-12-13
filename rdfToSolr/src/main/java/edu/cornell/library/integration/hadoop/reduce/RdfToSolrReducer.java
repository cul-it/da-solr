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
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.tdb.TDBFactory;

import edu.cornell.library.integration.indexer.IndexingUtilities;
import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFServiceException;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This reducer will take a key that is a URI of a bib and a list of
 * values that are strings of n-triple RDF and run a RDF to solr document
 * conversion on the RDF.  
 * 
 * THIS REDUCER DOES NOT WRITE THE DOCUMENT TO THE SOLR INDEX.
 */
public class RdfToSolrReducer extends Reducer<Text, Text, Text, Text> {
	Log log = LogFactory.getLog(RdfToSolrReducer.class);
	public final static String SOLR_SERVICE_URL = "integration.RdfToSolrReducer.SolrServiceUrl";
						
	File tmpDir;
	RecordToDocument r2d;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);		
		Configuration conf = context.getConfiguration();						
		
		tmpDir = Files.createTempDir();
		r2d = new RecordToDocumentMARC();
	}


	@Override	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {				
		
		try{						
			Model model = TDBFactory.createModel(tmpDir.getAbsolutePath());   
			
			for( Text value : values){			
				try{
					Text.validateUTF8( value.getBytes() );
					model.read(new StringReader(value.toString()),null,"N-TRIPLE");
				}catch(JenaException ex){
					log.error( "could not load RDF for " + key.toString(),ex);
					log.error( "Problem with RDF:\n" + value.toString() );
				}
			}			
			context.progress();
			
			try{
				SolrInputDocument doc = r2d.buildDoc(key.toString(), new RDFServiceModel(model));
				context.write(key, new Text( IndexingUtilities.toString(doc)));
				context.progress();
			}catch(Error er){
				log.error("Could not create solr document for " + key.toString() , er);				
			}
		} catch (Exception e) {
			log.error("Could not reduce " + key.toString(), e);
		}finally{
			Files.deleteDirectoryContents( tmpDir );			
		}
	}



	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		Files.deleteRecursively(tmpDir);
	}				
}