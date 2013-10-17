package edu.cornell.library.integration.indexer.updates;

import java.util.Map;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;
import edu.cornell.library.integration.indexer.IndexDirectory;

/**
 * Index all the MARC n-Triple BIB files for the incremental update. 
 * It will attempt to index the most recent MARC BIB n-Triples incremental
 * update file. 
 * 
 * This loads configuration properties using the VoyagerToSolrConfiguration.
 */
public class IncrementalBibFileToSolr {
    
    public static void main(String[] argv) throws Exception{
        
        VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(argv);
                                     
        /* Figure out the name of the most recent BIB MARC NT file. */
        String fileToIndex;
        String dirToLookIn = config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir() ; 
        try {
            fileToIndex = FileNameUtils.findMostRecentFile(
                    DavServiceFactory.getDavService(config), 
                    dirToLookIn,
                    config.getDailyMrcNtFilenamePrefix(), ".nt.gz");
        } catch (Exception e) {
            throw new Exception("Could not find most recent file in directory " 
                    + dirToLookIn, e);
        }                       
                
        if( fileToIndex == null )
            throw new Exception("Could not find most recent file in directory " 
                    + dirToLookIn );
        
        System.out.println("Attemping to load from " + fileToIndex );
        System.out.println("      to Solr Index at " + config.getSolrUrl());
                         
        /* Do the document building and indexing. */
        
        IndexDirectory indexer = new IndexDirectory();
        try{
            /* Setup indexer with properties from config. */                
            indexer.setDavUser(config.getWebdavUser());
            indexer.setDavPass(config.getWebdavPassword());
            indexer.setSolrURL(config.getSolrUrl());     
            
            indexer.setInputsURL( fileToIndex );
            indexer.indexDocuments();
        }catch(Exception e){
            throw new Exception("Problem while indexing documents from '" + fileToIndex + "'", e);
        }
        
        checkForErrors( indexer.getRecordWriter().getRecords() );
    }


    private static void checkForErrors( Map<String,String> records) throws Exception{
        if( records == null || records.isEmpty() ){
            throw new Exception("No records found for indexer, there may have been no inputs or there may have been a problem.");
        }
        
        String foundError = "";        
        
        for( String key: records.keySet()){
            
            String  value = records.get(key);
            if(value == null ){
                foundError += "No record for " + key + '\n';
            }else if( value.contains("ERROR") ){
                foundError += key + '\t' + value + '\n';
            }                        
        }
        
        if( ! foundError.trim().isEmpty() ){
            System.out.println( foundError );
            throw new Exception("There were errors during indexing.");
        }
        
    }
}
