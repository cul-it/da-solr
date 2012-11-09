package edu.cornell.library.integration.hadoop;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MarcToSolrDocs extends Configured implements Tool {

	public static String MARC_XML_URL = "mapreduce.MarcToSolrDocs.marcXmlUrl";
	public static String HOLDINGS_SPARQL_URL = "mapreduce.MarcToSolrDocs.holdingsSparqlUrl";
	
	@Override
	public int run(String[] args) throws Exception {
		//we'll need some args:
		// 
		// Input:
		// URL of MARC XML source 
		// holdings SPARQL URL
		
		// Output:
		// Directory to output solr XML documents to.
		  if (args.length < 3) {
		      System.out.println("MarcToSolrDocs <URLofMarcXML> <URLofHoldingsSparql> <outputDir>");
		      ToolRunner.printGenericCommandUsage(System.out);
		      return 2;
		  }
		  
		  //can we use side files for this?
		  // http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html#Task+Side-Effect+Files
		  // FileOutputFormat.getWorkOutputPath()
		  
		  Path tempDir =
		      new Path("marctosolrdocs-temp-"+
		          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		    Configuration conf = getConf();
		    conf.set(MARC_XML_URL, args[0]);
		    conf.set(HOLDINGS_SPARQL_URL, args[1] );
		    Job marcJob = new Job(conf);		    
		    
		    try {		      
		      marcJob.setJobName("marc-to-solr-docs");

		      FileInputFormat.setInputPaths(marcJob, args[0]);

		      marcJob.setMapperClass(MarcToSolrDocsMapper.class);

		      marcJob.setCombinerClass(LongSumReducer.class);
		      marcJob.setReducerClass(LongSumReducer.class);

		      FileOutputFormat.setOutputPath(marcJob, tempDir);
		      marcJob.setOutputFormatClass(SequenceFileOutputFormat.class);
		      marcJob.setOutputKeyClass(Text.class);
		      marcJob.setOutputValueClass(LongWritable.class);

		      marcJob.waitForCompletion(true);		      
		    }
		    finally {
		      FileSystem.get(conf).delete(tempDir, true);
		    }
		    return 0;
	} 

}
