package edu.cornell.library.integration.indexer.updates;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.IndexDirectory;

/**
 * Index all the MARC n-Triple BIB files for the incremental update. 
 * It will attempt to index the most recent MARC BIB n-Triples incremental
 * update file. 
 * 
 * After the indexing this will do a hard and soft commit.
 * 
 * This loads configuration properties using the VoyagerToSolrConfiguration.
 */
public class IncrementalBibFileToSolr {
    
    public static void main(String[] argv) throws Exception{
    	List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
    	requiredArgs.add("dailyMrcNtDir");
    	requiredArgs.add("solrUrl");
    	SolrBuildConfig config = SolrBuildConfig.loadConfig(argv,requiredArgs);
    	new IncrementalBibFileToSolr( config );

    }
     
    public IncrementalBibFileToSolr( SolrBuildConfig config ) throws Exception {
                                     
        /* Figure out the name of the most recent BIB MARC NT file. */
        List<String> filesToIndex;
        DavService davService = DavServiceFactory.getDavService(config);
        String dirToLookIn = config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir() ; 
        try {
            filesToIndex = davService.getFileUrlList(dirToLookIn);
        } catch (Exception e) {
            throw new Exception("Could not find most recent file in directory " 
                    + dirToLookIn, e);
        }                       
                
        if(( filesToIndex == null ) || filesToIndex.isEmpty())
            throw new Exception("Could not find most recent file in directory " 
                    + dirToLookIn );
        
        System.out.println(filesToIndex.size() + " N-Triples batch files found.");
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        
        int batchNo = 0;
        for (String fileToIndex: filesToIndex) {
	        System.out.println("Attemping to load from " + fileToIndex );
	        System.out.println("      to Solr Index at " + config.getSolrUrl());
	        System.out.println("         this is batch " + ++batchNo + " out of " + filesToIndex.size());
	        System.out.println("           starting at " + dateFormat.format(Calendar.getInstance().getTime()));
	                         
	        /* Do the document building and indexing. */
	        
	        IndexDirectory indexer = new IndexDirectory();
	        try{
	            /* Setup indexer with properties from config. */
	            indexer.setTmpDir( config.getTmpDir() );
	            indexer.setDavUser(config.getWebdavUser());
	            indexer.setDavPass(config.getWebdavPassword());
	            indexer.setSolrURL(config.getSolrUrl());
	            indexer.setSolrBuildConfig(config);
	            
	            indexer.setInputsURL( fileToIndex );
	
	            //ARQ.setExecutionLogging(Explain.InfoLevel.ALL) ;
	
	            indexer.indexDocuments();            
	            
	        }catch(Exception e){
	        	e.printStackTrace();
	            throw new Exception("Problem while indexing documents from '" + fileToIndex + "'", e);
	        }
	        
	        checkForErrors( indexer.getRecordWriter().getRecords() );

        }
        System.out.println("All updates batches loaded as of " + dateFormat.format(Calendar.getInstance().getTime()));

    }


    private static void checkForErrors( Map<String,String> records) throws Exception{
        if( records == null || records.isEmpty() ){
            throw new Exception("No records found for indexer, there may have been no inputs "
                                +"or there may have been a problem.");
        }

        String foundError = "";

        for( String key: records.keySet()){

            String  value = records.get(key);
            if(value == null ){
                foundError += "No record for " + key + '\n';
            }else if( value.toLowerCase().contains("error") ){
                foundError += key + '\t' + value + '\n';
            }                        
        }
        
        if( ! foundError.trim().isEmpty() ){
            System.out.println( foundError );
            throw new Exception("There were errors during indexing.");
        }
        
    }
}
