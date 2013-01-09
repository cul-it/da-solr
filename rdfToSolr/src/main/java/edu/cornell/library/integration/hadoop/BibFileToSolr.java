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
 * indexes the docs to solr.
 * 
 * The files in the inputDir should have one URL per line and those URLs should 
 * be the locations of bib.
 * 
 * 
 *  
 * 
 */
public class BibFileToSolr extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BibFileToSolr(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 3 ) {			
			System.out.println("\nBibFileToSolr <inputDir> <outputDir> <solrURL>");
			System.out.println("The inputDir should have files with one URL per line, Each URL should be the location of a N-Triple file with bib data");
			System.out.println("The outputDir should not exist before running.\n");
			ToolRunner.printGenericCommandUsage(System.out);
			System.out.println("");
			return 2;
		}
		
		Path inputDir= new Path( args[0] );
		Path outputDir= new Path( args[1] );
		String solrUrl = args[2];
		
		Configuration conf = getConf();
		conf.set(RdfToSolrReducer.SOLR_SERVICE_URL, solrUrl);
		
		Job job = new Job(conf);		    
				      						
		job.setJobName("BibFileToSolr");			
		job.setJarByClass( BibFileToSolr.class );
				
		TextInputFormat.setInputPaths(job, inputDir );			
		
		job.setMapperClass( BibFileIndexingMapper.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, outputDir);		
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);				     
			
		job.submit();	
		return 0;
	} 
}
