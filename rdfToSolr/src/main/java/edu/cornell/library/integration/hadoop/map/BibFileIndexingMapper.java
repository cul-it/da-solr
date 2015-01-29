package edu.cornell.library.integration.hadoop.map;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.io.Files;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;

import edu.cornell.library.integration.hadoop.BibFileToSolr;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceImpl;
import edu.cornell.library.integration.indexer.RecordToDocument;
import edu.cornell.library.integration.indexer.RecordToDocumentMARC;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;
import edu.cornell.mannlib.vitro.webapp.rdfservice.impl.jena.model.RDFServiceModel;

/**
 * This Mapper expects each item to have a URL of a bib or holdings N-Triples file.
 * For each <K,URL> this will load the RDF from the URL and index a Solr document 
 * each bib record found in the RDF.
 *   
 * @author bdc34
 *
 * @param <K> the incoming key is not used
 */
public class BibFileIndexingMapper <K> extends Mapper<K, Text, Text, Text>{
	Log log = LogFactory.getLog(BibFileIndexingMapper.class);
	
	protected boolean debug = false;
	
    //hadoop directory for the input splits that are completed 
    Path doneDir;
	
	String solrURL;
	SolrServer solr;

	Connection voyager;

    /** If true, attempt to delete the document from solr before adding them
        in order to do an update. */
    public boolean doSolrUpdate = false;

    /** Number of attempts if there are any exceptions during the mapping step. */
    public int attempts = 4;

	//model for data that gets reused each reduce
	Model baseModel;

    //URL of the WEBDAV service
	DavService davService;		

	//true of an error happened during a single call to map()
	boolean errors_encountered = false;

   
	
	public void map(K unused, Text urlText, Context context) throws IOException, InterruptedException {
		errors_encountered = false;
		
        String url = urlText.toString();
        if( url == null || url.trim().length() == 0 ) 
            return; //skip blank lines

        for (int i = 0; i < attempts; i++) { // In case of trouble, retry 

        	File tmpDir = Files.createTempDir();
			log.info("Using tmpDir " + tmpDir.getAbsolutePath() + " for file based RDF store.");
			
			Dataset dataset = null;
			Model model = null;
			
			try{			
				
				context.progress();
				
				log.info("Starting to build model");			
				//load the RDF to a triple store							
				dataset = TDBFactory.createDataset(tmpDir.getAbsolutePath()) ;
				model = dataset.getDefaultModel();

				model.add(baseModel);

				TDB.sync( dataset );

				// We are using the TDBLoader directly because of the use of 
				// zipped InputStream. It seems in the newer version of Jena (2.10)
				// there might be a more standard way to do this
				// see http://jena.apache.org/documentation/io/rdf-input.html
				
				//TDBLoader loader = new TDBLoader() ;
				//InputStream is = getUrl( urlText.toString()  );
				//loader.loadGraph((GraphTDB)model.getGraph(), is);

				String urlString = urlText.toString();
				InputStream is = getUrl( urlString  );

				Lang l ;
				if (urlString.endsWith("nt.gz") || urlString.endsWith("nt"))
					l = Lang.NT;
				else if (urlString.endsWith("n3.gz") || urlString.endsWith("n3"))
					l = Lang.N3;
				else
					throw new IllegalArgumentException("Format of RDF file not recogized: "+urlString);
				RDFDataMgr.read(model, is, l);
				context.progress();
								
				is.close();
				
				TDB.sync( dataset );
			
				log.info("Model load completed. Creating connection to Voyager Database.");
				voyager = openConnection();
				
				log.info("Starting query for all bib records in model. ");									
				Set<String> bibUris = getURIsInModel( model);
                int total = bibUris.size();

				context.progress();										 
				
				log.info("Starting to index documents");
				RDFService rdf = new RDFServiceModel(model);
                int n = 0;

                Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
                for( String bibUri: bibUris){	
                    n++;
                    System.out.println("indexing " + n + " out of " + total 
                                       + ". bib URI: " + bibUri );

                    //Create Solr Documents
                    try{
						SolrInputDocument doc = indexToSolr(bibUri, rdf);
						docs.add(doc);
						context.progress();
					}catch(Throwable ex ){
						ex.printStackTrace();
						context.write(new Text(bibUri), new Text("URI\tError\t"+ex.getMessage()));
                        if( checkForOutOfSpace( ex ) ){
                            return;
                        }
					}
                }
				
				try {
					voyager.close();
				}
				catch (SQLException SQLEx) { /* ignore */ }
				
				try{
					if ( ! docs.isEmpty() ) {
						if( doSolrUpdate ){
							//in solr an update is a delete followed by an add
			            	List<String> ids = new ArrayList<String>();
			            	for (SolrInputDocument doc : docs)
			            		ids.add((String)doc.getFieldValue("id"));
			            	solr.deleteById(ids);
			            }
						solr.add(docs);				
					}
	
					context.getCounter(getClass().getName(), "bib uris indexed").increment(docs.size());
					for (SolrInputDocument doc : docs)
						context.write(new Text(doc.get("id").toString()), new Text("URI\tSuccess"));
	
				} catch (Throwable er) {			
					throw new Exception("Could not add documents to index. Check logs of solr server for details.", er );
				}

								
				//attempt to move file to done directory when completed
				moveToDone( context , urlText.toString() );				

			}catch(Throwable th){			
				String filename = getSplitFileName(context);
				String errorMsg = "could not process file URL " + urlText.toString() +
						" due to " + th.toString() ;
				log.error( errorMsg );
				context.write( new Text( filename), new Text( "FILE\tError\t"+errorMsg ));
				continue; //failed... retry

			}finally{			
				FileUtils.deleteDirectory( tmpDir );			
				model.close();
				dataset.close();
			}
			
			return; // success, break out of loop
        }
	}
	

	// Open Connection to the Voyager Oracle Database
	@SuppressWarnings("unused")
	public static Connection openConnection() {
        Connection connection = null;
        
        String DBDriver = "oracle.jdbc.driver.OracleDriver";
 //       String DBUrl = "jdbc:oracle:thin:@database.library.cornell.edu:1521:VGER";
        String DBProtocol = "jdbc:oracle:thin:@";
        String DBServer = "database.library.cornell.edu:1521:VGER";
 //       String DBName = "CORNELLDB";
        String DBUser = "login";
        String DBPass = "login";
        
        if (false) {
        	DBDriver = "com.mysql.jdbc.Driver";
        	DBProtocol = "jdbc:mysql://";
        	DBServer = "fbw4-dev.library.cornell.edu:3306/item_data";
        	DBUser = "dna";
        	DBPass = "dna password";
        }
        
        
        // actually connect to the database
        try {

           Class.forName(DBDriver);
           String dburl = DBProtocol + DBServer;
           // System.out.println("database connection url: "+dburl);
           connection = DriverManager.getConnection(dburl , DBUser, DBPass);

           if (connection == null) {
              System.out.println("openconnection: no connection made");
           }
           // end alert if no connection made
        } catch (SQLException sqlexception) {
           System.out.println(sqlexception.getMessage());
           sqlexception.printStackTrace();
        } catch (Exception exception) {
           //System.out.println(exception);
           exception.printStackTrace();
        }

        return connection;
     }

	
    /**
     * Check if ex is an out of space exception.
     */
    private boolean checkForOutOfSpace( Throwable ex){
        return ex != null 
            && ex.getMessage() != null 
            && ex.getMessage().toLowerCase().contains("no space left on device");
    }

	/** Move the split from the todo directory to the done directory. 
	 * @throws InterruptedException */ 
	private void moveToDone( Context context , String fileUrl) throws java.io.IOException, InterruptedException{
	    try {
	        // skip moveToDone if special value is set for doneDir
	        if( DO_NOT_MOVE_TO_DONE.equals( context.getConfiguration().get(BibFileToSolr.DONE_DIR) )) {
	            return;
	        }else{
	            String filename = getSplitFileName(context);
	            FileSystem fs = FileSystem.get( context.getConfiguration() );
	            FileUtil.copy(fs, new Path(filename),fs, doneDir,
	                          true, false, fs.getConf()); 
	        }
        } catch (FileNotFoundException e) {
            // This error is likely caused by the file being moved when it was completed by another worker
            // let's not restart the indexing process in response.
            String filename = getSplitFileName(context);
            String errorMsg = "Processed file URL but could not move from "
                    + "todo directory to done directory for " + fileUrl +
                    " due to " + e.toString() ;
            log.warn( errorMsg );
            context.write( new Text( filename), new Text( "FILE\tError\t"+errorMsg ));
        }	    	            
    }

	private SolrInputDocument indexToSolr(String bibUri, RDFService rdf) throws Exception{
		SolrInputDocument doc=null;
		try{
			RecordToDocument r2d = new RecordToDocumentMARC();
			doc = r2d.buildDoc(bibUri, rdf, voyager);
			if( doc == null ){
				throw new Exception("No document created for " + bibUri);				
			}
			//if (debug) System.out.println(IndexingUtilities.prettyFormat( ClientUtils.toXML( doc ) ));
		}catch(Throwable er){			
			throw new Exception ("Could not create solr document for " +bibUri, er);			
		}
		return doc;
	}
	
	
	
	private InputStream getUrl(String url) throws IOException {
		InputStream is = null;
		try {
			is = davService.getFileAsInputStream(url);
			if( url.endsWith( ".gz" ) || url.endsWith(".gzip"))
				return new GZIPInputStream( is );
			else
				return is;
		} catch (Exception e) {
			throw new IOException("Could not get " + url , e);
		}		
	} 

	/** Attempt to get all the bib record URIs from model. */
	private Set<String> getURIsInModel(  Model model ) {
		Set<String>bibUris = new HashSet<String>();
		for( String queryStr : idQueries){
			try{
				Query query = QueryFactory.create(queryStr) ;
				QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
				try {
					ResultSet results = qexec.execSelect() ;
					for ( ; results.hasNext() ; ) {
				      QuerySolution soln = results.nextSolution() ;
				      Resource r = soln.getResource("URI") ; //result variable must be a resource
				      bibUris.add( r.getURI() );
				      if (debug)
				    	  log.info("URI in file: "+r.getURI() );
				    }
				  } finally { qexec.close() ; }
			}catch(com.hp.hpl.jena.query.QueryParseException ex){
				log.error("Could not parse query for URIs, " + ex.getMessage() + " \n" + queryStr);
			}
		}		
		log.info("Number of URIs found: " + bibUris.size());
		return bibUris;
	}

	/** get the filename for the current file (aka input split) that is being worked on. 
	 * @throws InterruptedException 
	 * @throws IOException */
    private String getSplitFileName(Context context) throws IOException, InterruptedException{
    	org.apache.hadoop.mapreduce.InputSplit split = context.getInputSplit();
        if( split instanceof  FileSplit ){
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            return fileSplit.getPath().getName();			
        }else{
            String[] locs =  split.getLocations();
            if( locs != null && locs.length > 0 ){
                return split.getLocations()[0];
            }else{
                return "no_split_location_or_file_found";
            }
        }
    }

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		Configuration conf = context.getConfiguration();
				
		doneDir = new Path( conf.get(BibFileToSolr.DONE_DIR) );
		if(doneDir == null )
			throw new Error("Requires directory of HDFS for the done directory in configuration, " +
					"this should have been set by BibFileToSolr or the parent hadoop job. "
					+ "Set it to " + BibFileIndexingMapper.DO_NOT_MOVE_TO_DONE + " to disable "
					+ "the done directory feature. "
					+ "It was " + BibFileToSolr.DONE_DIR);
		
		solrURL = conf.get( BibFileToSolr.SOLR_SERVICE_URL );
		if(solrURL == null )
			throw new Error("Requires URL of Solr server in config property " 
					+ BibFileToSolr.SOLR_SERVICE_URL);
				
		//solr = new ConcurrentUpdateSolrServer(solrURL, 100, 2);
        solr = new HttpSolrServer(solrURL);
		
		try { solr.ping(); } catch (SolrServerException e) {
			throw new Error("Cannot connect to solr server at \""+solrURL+"\".",e);
		}
		
		baseModel = loadBaseModel( context );
						
		davService = new DavServiceImpl(
				conf.get(BibFileToSolr.BIB_WEBDAV_USER),
				conf.get(BibFileToSolr.BIB_WEBDAV_PASSWORD));

        //set tmp directory system prop
        if( conf.get(BibFileToSolr.TMP_DIR) != null ){
            System.setProperty("java.io.tmpdir", conf.get(BibFileToSolr.TMP_DIR));
        }
	}
	
	private Model loadBaseModel(Context context) throws IOException {		
		Model baseModel = ModelFactory.createDefaultModel();		
		String[] baseNtFiles = { "/shadows.nt","/library.nt","/language_code.nt", "/callnumber_map.nt","/fieldGroups.nt"};
		for( String fileName : baseNtFiles ){		    
			InputStream in = getClass().getResourceAsStream(fileName);
			if( in == null )
			    throw new IOException("While attempting to load base RDF files, could not find resource " + fileName);
			baseModel.read(in, null, "N-TRIPLE");
			in.close();
			log.info("loaded base model " + fileName);
		}		
		return baseModel;
	}		
	
	//queries to get URIs from model, expected to variable named URI in result set
	protected static final List<String> idQueries = Arrays.asList(
			//query for bib IDs from bib MARC
			"SELECT DISTINCT ?URI WHERE {\n" +
			"  ?URI " +
			"  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
			"  <http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord> " +
			"}"
//			//query for bib IDs from holdings MARC
//			,"SELECT ?URI WHERE {\n" +
//			"  ?holdingUri " +
//			"  <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> ?URI" +
//			" }" 						
	);				

    //If BibFileToSolr.DON_DIR is set to this value
    // then the input splits will not be moved to the done directory.
    // this is just for testing, do not use it if you want to be able to 
    // restart a job. 
    public final static String DO_NOT_MOVE_TO_DONE = "DO_NOT_MOVE_TO_DONE";

    @SuppressWarnings("unchecked")
	public Context testContext(Configuration configuration,
            TaskAttemptID taskAttemptID, RecordReader<Text, Text> recordReader,
            RecordWriter<Text, Text> recordWriter, OutputCommitter outputCommitter,
            StatusReporter statusReporter, InputSplit inputSplit) throws IOException, InterruptedException {
        return new Context (configuration, taskAttemptID, (RecordReader<K, Text>) recordReader, recordWriter, outputCommitter, statusReporter, inputSplit);
    }
}
