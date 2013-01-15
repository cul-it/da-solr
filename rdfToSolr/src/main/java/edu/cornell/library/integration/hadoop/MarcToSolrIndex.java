package edu.cornell.library.integration.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cornell.library.integration.hadoop.map.BibFileIndexingMapper;
import edu.cornell.library.integration.hadoop.reduce.RdfToSolrIndexReducer;
import edu.cornell.library.integration.hadoop.reduce.RdfToSolrReducer;

/**
 * This is a hadoop job that is intended to load all the bib and holdings and
 * load them to the Solr index.
 * 
 * The files in the inputDir should have one URL per line and those URLs should 
 * be the locations of bib or holdings n-triple files.
 * 
 * The map and reduce are:
 *  map: for each URLs load the RDF and make <BIBURI -> RDF in N-TRIPLE String> mappings
 *  reduce: For each BIBURI key, make a local RDF triple store and run MarcToSolrDoc 
 * 
 * @author bdc34
 *
 */

public class MarcToSolrIndex extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarcToSolrIndex(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 2 ) {			
			System.out.println("MarcToSolrIndex <inputDir> <solrServerURL>");
			System.out.println("The inputDir should have files with one URL per line, Each URL should be the location of a N-Triple file with bib or holding data");
			System.out.println("The solrServerURL is the service to index the documents into.\n");
			ToolRunner.printGenericCommandUsage(System.out);
			System.out.println("");
			return 2;
		}
		
		Path inputDir= new Path( args[0] );
		String solrUrl = args[1];
		
		Configuration conf = getConf();
		
		conf.set(RdfToSolrReducer.SOLR_SERVICE_URL, solrUrl);
		
		Job job = new Job(conf);		    
				      		    
		job.setJobName("MarcToSolrIndex");			
		job.setJarByClass( MarcToSolrDocs.class );
					
		TextInputFormat.setInputPaths(job, inputDir );			
		
		job.setMapperClass(BibFileIndexingMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass( RdfToSolrIndexReducer.class );
						
		//we don't really have output but it seems that this is needed.
		FileOutputFormat.setOutputPath(job, new Path("./output"));		
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.submit();		
		return 0;
	} 
}