package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.cornell.library.integration.hadoop.BibFileToSolr;
import edu.cornell.library.integration.hadoop.map.BibFileIndexingMapper;
import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Index all the files in a given WEBDAV directory. It is intended as a simple way to 
 * index a set of MARC n-Triple files.
 * 
 * This uses the BibFileIndexingMapper and provides basic implementations of the
 * Haddop classes to drive it. 
 * 
 */
public class IndexDirectory {

    /**
     * DavUser name
     */
    String davUser;
    
    /**
     * Dav user password
     */
    String davPass;
    
    /**
     * URL of WEBDAV directory where input files can be found.
     */
    String inputsURL;
    
    /**
     * URL of Solr service to write Documents to.
     */
    String solrURL;

    /**
     * Tmp directory. if not null, the value will
     * replace the system property java.io.tmpdir
     */
    String tmpDir;

    /**
     * Results from Hadoop are sent to recordWriter.
     * This could be used to get results about how each
     * record was processed.  
     */
    MockRecordWriter recordWriter;   
    
    SolrBuildConfig config;

    public void setSolrBuildConfig( SolrBuildConfig conf ) {
    	config = conf;
    }
    
 	public String getDavUser() {
        return davUser;
    }

    public void setDavUser(String davUser) {
        this.davUser = davUser;
    }

    public String getDavPass() {
        return davPass;
    }

    public void setDavPass(String davPass) {
        this.davPass = davPass;
    }

    public String getInputsURL() {
        return inputsURL;
    }

    public void setInputsURL(String inputsURL) {
        this.inputsURL = inputsURL;
    }

    public String getSolrURL() {
        return solrURL;
    }

    public void setSolrURL(String solrURL) {
        this.solrURL = solrURL;
    }

    public String getTmpDir(){
        return tmpDir;
    }
    
    public void setTmpDir(String td){
        this.tmpDir = td;
    }

    public MockRecordWriter getRecordWriter() {
        return recordWriter;
    }

    /**
 	 * Run the BibFileIndexingMapper on the file from WEBDAV URL
 	 * and index the results in solrSeviceUrl. 
 	 * 
 	 */
    public void indexDocuments( ) 
            throws IOException, InterruptedException{


        MockInputSplit inputSplit = new MockInputSplit(inputsURL);            
        MockOutputCommitter outputCommitter = new MockOutputCommitter();
        MockStatusReporter statusReporter = new MockStatusReporter();
        recordWriter = new MockRecordWriter();
        MockRecordReader recordReader = new MockRecordReader();

        BibFileIndexingMapper<Object> indexingMapper = new BibFileIndexingMapper<Object>();
        indexingMapper.doSolrUpdate = true; 
        indexingMapper.attempts = 1;
                
        Mapper<Object,Text,Text,Text>.Context context = 
            indexingMapper.testContext(new Configuration(), new TaskAttemptID(),
                    recordReader, recordWriter, outputCommitter, statusReporter, inputSplit);           
        
        Configuration hadoopConfig = context.getConfiguration();
        hadoopConfig.set( BibFileToSolr.DONE_DIR, BibFileIndexingMapper.DO_NOT_MOVE_TO_DONE);
        hadoopConfig.set( BibFileToSolr.SOLR_SERVICE_URL, solrURL);
        hadoopConfig.set( BibFileToSolr.BIB_WEBDAV_USER, davUser);
        hadoopConfig.set( BibFileToSolr.BIB_WEBDAV_PASSWORD, davPass);
        
       	if (config != null) {
       		hadoopConfig = config.valuesToHadoopConfig(hadoopConfig);
       	}

        if( tmpDir != null )
            hadoopConfig.set( BibFileToSolr.TMP_DIR, tmpDir);

        indexingMapper.setup(context);

        indexingMapper.map(null, new Text(inputsURL), context);

        //can access the result records with recordWriter if needed
        //but right now they not used
        //they would be useful to get error reporting etc.
    }

    public final static class MockRecordWriter extends RecordWriter<Text, Text> {
        
        HashMap<String,String> results = new HashMap<String,String>();
        
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException { 
            //nothing to do here
        }
        
        public HashMap<String,String> getRecords(){ 
            return results; 
        }
        
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            results.put(key.toString(), value.toString());
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