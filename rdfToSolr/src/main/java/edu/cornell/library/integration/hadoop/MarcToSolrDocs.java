package edu.cornell.library.integration.hadoop;

import java.io.File;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarcToSolrDocs extends Configured implements Tool {

	public static String MARC_N3_URL = "mapreduce.MarcToSolrDocs.marcN3Url";
	public static String HOLDINGS_SPARQL_URL = "mapreduce.MarcToSolrDocs.holdingsSparqlUrl";
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MarcToSolrDocs(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length < 3) {
			System.out.println("MarcToSolrDocs <URLofMarcN3> <URLofHoldingsSparql> <outputDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return 2;
		}

		String urlOfMarcN3Index = args[0];
		String holdingSparqlUrl = args[1];
		Path outputDir= new Path( args[2] );

		//can we use side files for this? 
		// http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html#Task+Side-Effect+Files
		// FileOutputFormat.getWorkOutputPath()
		int randomId = new Random().nextInt(Integer.MAX_VALUE);
		Path tempDir =
			new Path("marctosolrdocs-temp-"+Integer.toString(randomId));
		File f = new File( tempDir.getName() );
		f.mkdir();
		
		Configuration conf = getConf();
		conf.set(MARC_N3_URL, urlOfMarcN3Index );
		conf.set(HOLDINGS_SPARQL_URL, holdingSparqlUrl);
		Job marcJob = new Job(conf);		    

		try {		      		     
			marcJob.setJobName("marc-to-solr-docs");

			//get the Bib record URLs and save them 
			String bibUrlsFileName = tempDir.getName() + "/" + Integer.toString(randomId) + "bibUrls.txt";
			MarcToSolrUtils.saveLinksToFile( urlOfMarcN3Index, bibUrlsFileName); 

			FileInputFormat.setInputPaths(marcJob, new Path( bibUrlsFileName));		      

			marcJob.setMapperClass(MarcToSolrDocsMapper.class);

			//marcJob.setCombinerClass(LongSumReducer.class);

			marcJob.setNumReduceTasks(0);		      

			FileOutputFormat.setOutputPath(marcJob, outputDir);
			//marcJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			marcJob.setOutputKeyClass(Text.class);
			marcJob.setOutputValueClass(Text.class);

			marcJob.waitForCompletion(true);		      
		}
		finally {
			FileSystem.get(conf).delete(tempDir, true);
		}
		return 0;
	} 

}
