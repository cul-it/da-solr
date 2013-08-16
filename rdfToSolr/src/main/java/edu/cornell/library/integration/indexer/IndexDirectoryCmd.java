package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.cornell.library.integration.hadoop.BibFileToSolr;
import edu.cornell.library.integration.hadoop.map.BibFileIndexingMapper;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;

/**
 * Index all the files in a given directory.
 */
public class IndexDirectoryCmd extends CommandBase {


 	public static void main(String [] args) throws IOException, InterruptedException{				
		if( help( args ) ) 
			return;
        
		String sourceDir = args[0];
		String solrServiceUrl = args[1];
        String davUser = "what?";
        String davPass = "meworry?";
            
        DavService davService = new DavServiceImpl( davUser, davPass );

        List<String> bibUrls = davService.getFileList( sourceDir );

        for( String url : bibUrls){
            indexBibUrl( url, davUser, davPass, solrServiceUrl );
        }
    }

    protected static void indexBibUrl( String url, String davUser, String davPass, String solrServiceUrl) 
            throws IOException, InterruptedException{


        MockInputSplit inputSplit = new MockInputSplit(url);            
        MockOutputCommitter outputCommitter = new MockOutputCommitter();
        MockStatusReporter statusReporter = new MockStatusReporter();
        MockRecordWriter recordWriter = new MockRecordWriter();
        MockRecordReader recordReader = new MockRecordReader();

        BibFileIndexingMapper indexingMapper = new BibFileIndexingMapper();
        
        Mapper<Text,Text,Text,Text>.Context context = 
            indexingMapper.testContext(new Configuration(), new TaskAttemptID(),
                    recordReader, recordWriter, outputCommitter, statusReporter, inputSplit);           
        
        context.getConfiguration().set( BibFileToSolr.DONE_DIR, BibFileIndexingMapper.DO_NOT_MOVE_TO_DONE);
        context.getConfiguration().set( BibFileToSolr.SOLR_SERVICE_URL, solrServiceUrl);
        context.getConfiguration().set( BibFileToSolr.BIB_WEBDAV_USER, davUser);
        context.getConfiguration().set( BibFileToSolr.BIB_WEBDAV_PASSWORD, davPass);

        indexingMapper.setup(context);

        indexingMapper.map(null, new Text(url), context);

        //can access the result records with recordWriter

    }

	private static String help = 
        "\nIndexDirectoryCmd will convert all run the conversion on all files found " +
        "in the directory. A webDav URL is acceptable for the directory. \n" +
        "expected:  sourceDirectory solrIndexURL\n";
	private static boolean help(String[] args) {
		if( args == null || args.length != 2 ){
			System.err.print(help);
			return true;
		}else{
			return false;
		}
	}




    final static class MockRecordWriter extends RecordWriter<Text, Text> {
        
        ArrayList<Text> keys = new ArrayList<Text>();
        ArrayList<Text> values = new ArrayList<Text>();
        
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException { }
        
        public Text[] getKeys() {
            Text result[] = new Text[keys.size()];
            keys.toArray(result);
            return result;
        }
        public IntWritable[] getValues() {
            IntWritable[] result = new IntWritable[values.size()];
            values.toArray(result);
            return result;
        }
        @Override
        public void write(Text key, Text value) throws IOException,
                InterruptedException {
            keys.add(key);
            values.add(value);            
        }
    };  

    final static class MockRecordReader extends RecordReader<Text, Text> {
        public void close() throws IOException { }
        public Text getCurrentKey() throws IOException, InterruptedException {
            throw new RuntimeException("Tried to call RecordReader:getCurrentKey()");
        }
        public Text getCurrentValue() throws IOException, InterruptedException {
            throw new RuntimeException("Tried to call RecordReader:getCurrentValue()");
        }
        public float getProgress() throws IOException, InterruptedException {
            throw new RuntimeException("Tried to call RecordReader:getProgress()");
        }
        public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException { }
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return false;
        }
    };

    final static class MockStatusReporter extends StatusReporter {
        private Counters counters = new Counters();
        private String status = null;
        public void setStatus(String arg0) {
            status = arg0;
        }
        public String getStatus() {
            return status;
        }
        public void progress() { }
        public Counter getCounter(String arg0, String arg1) {
            return counters.getGroup(arg0).findCounter(arg1);
        }
        public Counter getCounter(Enum<?> arg0) {
            return null;
        }
    };

    final static class MockInputSplit extends InputSplit {
        String name;

        public MockInputSplit(String name){
            this.name = name;
        }

        public String[] getLocations() throws IOException, InterruptedException {
            String[] a = { name };
            return a;
        }
        public long getLength() throws IOException, InterruptedException {
            return 1;
        }
    };

    final static class MockOutputCommitter extends OutputCommitter {
        public void setupTask(TaskAttemptContext arg0) throws IOException { }
        public void setupJob(JobContext arg0) throws IOException { }
        public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
            return false;
        }
        public void commitTask(TaskAttemptContext arg0) throws IOException { }
        public void cleanupJob(JobContext arg0) throws IOException { }
        public void abortTask(TaskAttemptContext arg0) throws IOException { }
    };

}
