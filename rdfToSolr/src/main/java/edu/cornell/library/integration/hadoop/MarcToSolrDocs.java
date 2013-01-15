package edu.cornell.library.integration.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cornell.library.integration.hadoop.map.BibFileIndexingMapper;
import edu.cornell.library.integration.hadoop.reduce.RdfToSolrReducer;

/**
 * This is a hadoop job that is intended to load all the bib and holdings and
 * writes out solr docs in a file.
 * 
 * The files in the inputDir should have one URL per line and those URLs should 
 * be the locations of bib or holdings n-triple files.
 * 
 * The map and reduce are:
 *  map: for each URLs load the RDF and make <BIBURI -> N-TRIPLE String> mappings
 *  reduce: For each BIBURI key, make a local RDF triple store and run MarcToSolrDoc 
 * 
 */
public class MarcToSolrDocs extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarcToSolrDocs(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 2 ) {			
			System.out.println("\nMarcToSolrDocs <inputDir> <outputDir>");
			System.out.println("The inputDir should have files with one URL per line, Each URL should be the location of a N-Triple file with bib or holding data");
			System.out.println("The outputDir should not exist before running.\n");
			ToolRunner.printGenericCommandUsage(System.out);
			System.out.println("");
			return 2;
		}
		
		Path inputDir= new Path( args[0] );
		Path outputDir= new Path( args[1] );
		
		Configuration conf = getConf();
		
		//conf.set("mapred.compress.map.output", "true");		
		//conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		
		Job job = new Job(conf);		    
				      		    
		job.setJobName("MarcToSolrDocs");			
		job.setJarByClass( MarcToSolrDocs.class );
					
		TextInputFormat.setInputPaths(job, inputDir );			
		
		job.setMapperClass(BibFileIndexingMapper.class);
		job.setReducerClass( RdfToSolrReducer.class );

		FileOutputFormat.setOutputPath(job, outputDir);		
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);		      
			
		return 0;
	} 
}