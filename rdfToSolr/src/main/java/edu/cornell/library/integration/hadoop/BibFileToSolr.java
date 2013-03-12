package edu.cornell.library.integration.hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cornell.library.integration.hadoop.map.BibFileIndexingMapper;
import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;

/**
 * This is a hadoop job that is intended to load all the bib and holdings and
 * indexes the docs to solr.
 * 
 * The files in the inputDir should have one URL per line and those URLs should 
 * be the locations of bib.  If there are no files in inputDir the directory
 * listing will be fetched from WebDav and new URL files will be written to
 * inputDir.
 * 
 */
public class BibFileToSolr extends Configured implements Tool {
    
	public final static String INPUT_DIR = "BibFileToSolr.inputDir";
	public final static String TODO_DIR = "BibFileToSolr.todoDir";
    public final static String DONE_DIR = "BibFileToSolr.doneDir";
    public final static String SOLR_SERVICE_URL = "BibFileToSolr.solrServiceUrl";
   
    public final static String BIB_WEBDAV_URL = "BibFileToSolr.bibWebdavUrl";
    public final static String BIB_WEBDAV_USER = "BibFileToSolr.bibWebdavUser";
    public final static String BIB_WEBDAV_PASSWORD = "BibFileToSolr.bibWebdavPassword";
    
    public final static String HOLDING_SERVICE_URL= "BibFileToSolr.holdingServiceUrl";
    

	//holdingsIndex = new HoldingForBib("http://jaf30-dev.library.cornell.edu:8080/DataIndexer/showTriplesLocation.do");
	//admin password
    
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BibFileToSolr(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {				
		if (args.length != 1 ) 	
			return help();		
		
		Configuration conf = getConf();	
		
		//load the job configuration XML and add to hadoop's configuration
		String configXmlPath = args[0] ;					
		conf.addResource(new File ( configXmlPath ).getAbsoluteFile().toURI().toURL());
		conf.reloadConfiguration();
		System.err.println(conf);								
		
		checkConfig( conf );
		
		Path inputDir = new Path( conf.get( INPUT_DIR ));
		setupInputDir( conf, inputDir);
		
		//make a new output dir each run because FileOutputFormat needs that.
		Path outputDir = setupOutputDir( conf );				
		
		//set todo dir if there is none in conf.
		Path todoDir = null;
		if( conf.get(TODO_DIR) == null ){
			todoDir = new Path(inputDir.toString() + ".todo");		
			conf.set(TODO_DIR, todoDir.toString());			
		}else{
			todoDir =new Path( conf.get(TODO_DIR) );
			System.err.println("using TODO_DIR from configuration \"" + todoDir + "\" instead of default of \"" + inputDir.toString()+".todo\"");
			System.err.println("This is an unusual way to run the job and may prevent pervious work from being resumed.");
			System.err.println("Use with caution.");
		}
		
		//set the done dir if there is none in conf.  
		Path doneDir = null;// new Path(inputDir.toString() + ".done");		
		if( conf.get(DONE_DIR) == null ){
			doneDir = new Path(inputDir.toString() + ".done");		
			conf.set(DONE_DIR, doneDir.toString());			
		}else{
			doneDir =new Path( conf.get(DONE_DIR) );
			System.err.println("using DONE_DIR from configuration \"" + doneDir + "\" instead of default of \"" + inputDir.toString()+".done\"");
			System.err.println("This is an unusual way to run the job, use with caution.");
		}				
		
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
		System.err.println("job submitted");
		return 0;
	} 

	/** Each run we make a new output dir 
	 * @throws IOException */
	private Path setupOutputDir(Configuration conf) throws IOException {
		FileSystem fs =  FileSystem.get(conf);
    	
		// Make a directory to put all the output dirs into.
		Path dirForOutputs = new Path( conf.get( INPUT_DIR )+".outputs" );
		
    	if( ! dirExists( fs, dirForOutputs)){
    		fs.mkdirs( dirForOutputs );
    	}    	
    	
        Date now = new Date( );
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy.MM.dd.hh.mm.ss");
        
    	Path outputDir = new Path( dirForOutputs.toString() + Path.SEPARATOR + ft.format(now)  );
    	return outputDir;
	}

	private int help() {
		System.err.println("\nBibFileToSolr <configFile.xml>");
		System.err.println("see exampleBibFileToSolrConfig.xml for instrucitons on configFile.xml");
		ToolRunner.printGenericCommandUsage(System.out);
		System.err.println("");
		return 2;		
	}

	/**
	 * ensure that there is an inputDir, an outputDir and that the inputDir
	 * has bib URL files. 
	 */
    private void setupInputDir(Configuration conf, Path inputDir) throws IOException {
    	FileSystem fs =  FileSystem.get(conf);
    	
    	if( ! dirExists( fs, inputDir)){
    		fs.mkdirs( inputDir );    		
    	}    	
    	    	
    	FileStatus[] files = fs.listStatus( inputDir );
    	if( files != null && files.length > 0 ){
    		System.err.println("Found " + files.length + " input files in " + inputDir.toString() + ".");
    	}else{
    		System.err.println("No input files found in " + inputDir.toString() + ", fetching them from webdav.");
    		fetchInputFilesFromWebdav( conf, inputDir );
    	}		
	}

	/** Get a listing of files in WebDav and make one file per URL in inputDir.	
	 * @throws IOException */
	private void fetchInputFilesFromWebdav(Configuration conf, Path inputDir) throws IOException {
		FileSystem fs =  FileSystem.get(conf);
		Random r = new Random();
		try{
			DavService dav = new DavServiceImpl(conf.get( BIB_WEBDAV_USER ), conf.get(BIB_WEBDAV_PASSWORD));
			String davUrl = conf.get( BIB_WEBDAV_URL );
			List<String> files = dav.getFileList( conf.get( BIB_WEBDAV_URL ) );
			if( ! davUrl.endsWith("/")){
				davUrl = davUrl + "/";
			}
			for( String file : files ){	
				if( file.endsWith(".gz") ){
					file = file.substring(0,file.indexOf(".gz"));
					System.out.println( file );
				}
				String name = Integer.toString(r.nextInt(10000)) + file;		
				Path newf = new Path( inputDir, name);
				BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(newf,true)));
				br.write(  davUrl + file );
				br.close();							
			}
		}catch( IOException ex ){			
			throw new Error( "Cannot access BIB RDF webdav service at " + conf.get(BIB_WEBDAV_URL), ex);
		}
		
		
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
    
    /** Check that the properties that are needed are in the config. 
     *  Throw an Error if properties are missing. */
    private void checkConfig(Configuration config){
    	String[][] checks ={
    		{SOLR_SERVICE_URL,"required property " + SOLR_SERVICE_URL + " is not in configuraiton." }, 
    		{INPUT_DIR ,"required property " + INPUT_DIR+ " is not in configuraiton." } ,
    		{HOLDING_SERVICE_URL,"required property " + HOLDING_SERVICE_URL + " is not in configuraiton." },
    		{BIB_WEBDAV_PASSWORD ,"required property " + BIB_WEBDAV_PASSWORD + " is not in configuraiton." },
    		{BIB_WEBDAV_URL ,"required property " + BIB_WEBDAV_URL+ " is not in configuraiton." },
    		{BIB_WEBDAV_USER,"required property " + BIB_WEBDAV_USER + " is not in configuraiton." }    		
    	};
    	
    	for( String[] check : checks ){
    		if( config.get( check[0] ) == null ){
    			throw new Error( check[1] );
    		}
    	}    	    	
    }
}
