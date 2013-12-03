package edu.cornell.library.integration.indexer.updates;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.sparql.mgt.Explain;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;
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
            indexer.setTmpDir( config.getTmpDir() );
            indexer.setDavUser(config.getWebdavUser());
            indexer.setDavPass(config.getWebdavPassword());
            indexer.setSolrURL(config.getSolrUrl());     
            
            indexer.setInputsURL( fileToIndex );

            //ARQ.setExecutionLogging(Explain.InfoLevel.ALL) ;

            indexer.indexDocuments();            
            commitAndMakeAvaiableForSearch( config.getSolrUrl() );
            
        }catch(Exception e){
            throw new Exception("Problem while indexing documents from '" + fileToIndex + "'", e);
        }
        
        checkForErrors( indexer.getRecordWriter().getRecords() );
    }

    /**
     * Do a hard and soft commit. This should commit the changes to the index (hard commit)
     * and also open new searchers on the Solr server so the search results are
     * visible (soft commit). 
     */
    private static void commitAndMakeAvaiableForSearch(String solrUrl) 
            throws SolrServerException, IOException {
        SolrServer solr = new  HttpSolrServer( solrUrl );
        solr.commit(true,true,true);        
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
