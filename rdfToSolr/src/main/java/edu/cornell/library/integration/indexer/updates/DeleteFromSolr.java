package edu.cornell.library.integration.indexer.updates;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;

/**
 * Utility to delete bibs from Solr index.
 * 
 * Gets directory from config.getDailyBibDeleted() via WEBDAV. the most current
 * delete file will be used.
 * 
 * The file should have a single Bib ID per a line. It should be UTF-8 encoded.
 * 
 * A delete requests will be sent to the Solr service for each bib ID. They
 * may be sent in batches.
 */
public class DeleteFromSolr {
           
    public static void main(String[] argv) throws Exception{
        VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(argv);
        
        DeleteFromSolr dfs = new DeleteFromSolr();        
        dfs.doTheDelete(config);        
    }
    
    public void doTheDelete(VoyagerToSolrConfiguration config) throws Exception  {
            
            
        DavService davService = DavServiceFactory.getDavService( config );
        
        //  use the most current delete file        
        String prefix = "bibListForDelete";                  
        String deleteFileURL="notYetSet?";
        try {
            deleteFileURL = FileNameUtils.findMostRecentFile(davService, config.getDailyBibDeletes(), prefix);
        } catch (Exception e) {                        
            throw new Exception("No documents have been deleted, could not find the most recent deletes "
                    + "file from " + config.getWebdavBaseUrl() + "/" + config.getDailyBibDeletes() , e );
        }            
                
                
        String solrURL = config.getSolrUrl();                                        
        SolrServer solr = new HttpSolrServer( solrURL );        
                        
        int lineNum = 0;
        try{
            System.out.println("Deleteing BIB IDs found in " + deleteFileURL);
            InputStream is = davService.getFileAsInputStream( deleteFileURL );      
            BufferedReader reader = new BufferedReader(new InputStreamReader( is , "UTF-8" ));
    
            List<String> ids = new ArrayList<String>(100);
                        
            String line;   
            while ((line = reader.readLine()) != null)   {
                if( line != null && !line.trim().isEmpty()){
                    lineNum++;
                    ids.add( line );
                }
                
                if( ids.size() > 99 ){
                    solr.deleteById(ids, 1000 * 10 );
                    ids.clear();
                }                    
            }                        
        } catch (Exception e) {
            throw new Exception( "Exception while processing deletes form file " + deleteFileURL + 
                    ", problem around line " + lineNum + ", some documents may have been deleted from Solr.", e);
        }
        
        System.out.println("Success: deleted " + lineNum + " documents from solr.");
    }    
}
