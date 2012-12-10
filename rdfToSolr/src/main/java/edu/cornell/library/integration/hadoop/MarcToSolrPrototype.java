package edu.cornell.library.integration.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cornell.library.integration.hadoop.map.BibURIToRdfLocalMapper;
import edu.cornell.library.integration.hadoop.reduce.RdfToSolrReducer;

/**
 * This is a hadoop job is a test that uses a remote triple store
 * such as virtuoso or owl-im and builds Solr XML documents.
 * It does not attempt to put the solr document into an solr server.
 *    
 * 
 * @author bdc34
 *
 */
public class MarcToSolrPrototype extends Configured implements Tool {

	/* configuration property for URL of remote triple store */ 
	public static String REMOTE_SPARQL_ENDPOINT = "mapreduce.MarcToSolrDocs.remoteSparqlEndpoint";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarcToSolrPrototype(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length < 3) {
			System.out.println("MarcToSolrPrototype <URLofRemoteSparqlEndpoint> <URIListFile> <outputDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}
		
		String sparqlEndpointURL = args[0];	
		Path URIListFile = new Path(args[1]);
		Path outputDir= new Path( args[2] );
		
		Configuration conf = getConf();
		conf.set( REMOTE_SPARQL_ENDPOINT, sparqlEndpointURL);
		
		Job job = new Job(conf);		    				      		    
		job.setJobName("MarcToSolrPrototype");
					
		FileInputFormat.setInputPaths(job, URIListFile);
		
		//map that will build the RDF needed for Bib URL
		job.setMapperClass( BibURIToRdfLocalMapper.class ); 		     

		//reduce each list of RDF to a local store then a solr docuemnt
		job.setReducerClass( RdfToSolrReducer.class );
		
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);		      				
		return 0;
	} 

}
