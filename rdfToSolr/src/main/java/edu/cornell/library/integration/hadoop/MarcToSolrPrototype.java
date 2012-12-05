package edu.cornell.library.integration.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

		//can we use side files for our temporary files? 
		// http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html#Task+Side-Effect+Files
		// ??? FileOutputFormat.getWorkOutputPath() ???
		
//		int randomId = new Random().nextInt(Integer.MAX_VALUE);
//		Path tempDir =
//			new Path("marctosolrdocs-temp-"+Integer.toString(randomId));
//		File f = new File( tempDir.getName() );
//		f.mkdir();
		
		Configuration conf = getConf();
		conf.set( REMOTE_SPARQL_ENDPOINT, sparqlEndpointURL);		
		Job marcJob = new Job(conf);		    

		try {		      		     
			marcJob.setJobName("MarcToSolrPrototype");
						
			//file with one bib URI per line
			//This is where each of the original mappers get its starting point
			FileInputFormat.setInputPaths(marcJob, URIListFile);
			
			//add a map stage that will build the local RDF for a URL
			marcJob.setMapperClass( BibURIToRdfLocalMapper.class);

			//I think that this can be null or left unset
//			marcJob.setCombinerClass(???);

			//TODO: set the reducer to RDF->Solr 
//			marcJob.setNumReduceTasks(0);		      

			FileOutputFormat.setOutputPath(marcJob, outputDir);
//			//marcJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			marcJob.setOutputKeyClass(Text.class);
			marcJob.setOutputValueClass(Text.class);
			marcJob.waitForCompletion(true);		      
		}
		finally {
			//FileSystem.get(conf).delete(tempDir, true);
		}
		return 0;
	} 

}
