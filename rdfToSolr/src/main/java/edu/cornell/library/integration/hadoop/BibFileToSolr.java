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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.fs.FileStatus;
import java.io.FileNotFoundException;

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
    public static String TODO_DIR = "TODO_DIR";
    public static String DONE_DIR = "DONE_DIR";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BibFileToSolr(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 3 ) {			
			System.out.println("\nBibFileToSolr <inputDir> <outputDir> <solrURL>");
			System.out.println("The inputDir should have files with one URL per " +
			"line, Each URL should be the location of a N-Triple file with bib data");
			System.out.println("The outputDir should not exist before running.\n");
			ToolRunner.printGenericCommandUsage(System.out);
			System.out.println("");
			return 2;
		}
		
		Path inputDir= new Path( args[0] );
		Path outputDir= new Path( args[1] );
		String solrUrl = args[2];

		Path doneDir = new Path(inputDir.toString() + ".done");
        	Path todoDir = new Path(inputDir.toString() + ".todo");

		Configuration conf = getConf();

		conf.set(RdfToSolrReducer.SOLR_SERVICE_URL, solrUrl);
		conf.set(TODO_DIR, todoDir.toString());
	        conf.set(DONE_DIR, doneDir.toString());

        //setup todo and done directories
        setupTodoAndDone( FileSystem.get(conf), inputDir, todoDir, doneDir );

		Job job = new Job(conf);
				      						
		job.setJobName("BibFileToSolr");			
		job.setJarByClass( BibFileToSolr.class );
				
		TextInputFormat.setInputPaths(job, todoDir );			
		
		job.setMapperClass( BibFileIndexingMapper.class);
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, outputDir);		
		job.setOutputFormatClass( TextOutputFormat.class );
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);				     
			
		job.submit();	
		return 0;
	} 

    private Path setupTodoAndDone(FileSystem fs, 
                                  Path inputDir, 
                                  Path todoDir, Path doneDir) throws java.io.IOException {
        if( dirExists( fs, todoDir )){
            //assume that things are setup and use todo
            System.out.println("Resuming from todo directory: " + todoDir);
        } else {
            System.out.println("Since no todo directory was found at " 
                               + todoDir + " going to set up a new todo and done directory.");            
            fs.mkdirs( todoDir );
            fs.mkdirs( doneDir );

            //copy input to todo         
            Path[] workFiles = FileUtil.stat2Paths( fs.listStatus( inputDir ) );
            FileUtil.copy( fs, workFiles, fs, todoDir, false, false, fs.getConf() );
        }
        return todoDir;
    }

    private boolean dirExists( FileSystem fs, Path dir)throws java.io.IOException{
        try{
            FileStatus stat = fs.getFileStatus( dir );
            if( stat != null && stat.isDir() )
                return true;
            else 
                return false;
        }catch( FileNotFoundException fnoe){
            return false;
        }
    }
}
