package edu.cornell.library.integration.indexer.updates;

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
        IndexDirectory indexer = new IndexDirectory();
        
        /* Setup indexer with properties from config. */
        indexer.setDavUser(config.getWebdavUser());
        indexer.setDavPass(config.getWebdavPassword());
        indexer.setSolrURL(config.getSolrUrl());            
        
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
        
        indexer.setInputsURL( fileToIndex ); 
        
        /* Do the doucment building and indexing. */
        try{
            indexer.indexDocuments();
        }catch(Exception e){
            throw new Exception("Problem while indexing documents from '" + fileToIndex + "'", e);
        }
        
        //TODO: how do we get the results from indexer.getRecordWriter() ?
    }


}
