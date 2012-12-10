package edu.cornell.library.integration.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cornell.library.integration.hadoop.map.ExploreMapper;
import edu.cornell.library.integration.hadoop.map.URLToMarcRdfFetchMapper;


public class ExploreJob extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ExploreJob(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 2 ) {			
			System.out.println("\nExploreJob <inputDir> <outputDir>");			
			ToolRunner.printGenericCommandUsage(System.out);
			System.out.println("");
			return 2;
		}
		
		Path inputDir= new Path( args[0] );
		Path outputDir= new Path( args[1] );

		
		Configuration conf = getConf();
		Job job = new Job(conf);		    

		try {		      		     
			job.setJobName("ExploreJob");			
			job.setJarByClass( ExploreJob.class );
						
			
			TextInputFormat.setInputPaths(job, inputDir );			
			job.setMapperClass(ExploreMapper.class);
			
//			job.setCombinerClass(???);
			job.setNumReduceTasks(0);		      

			FileOutputFormat.setOutputPath(job, outputDir);
//			//job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.waitForCompletion(true);		      
		}
		finally {
			//FileSystem.get(conf).delete(tempDir, true);
		}
		return 0;
	} 

}
