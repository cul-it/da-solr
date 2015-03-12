package edu.cornell.library.integration.indexer.updates;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
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
        SolrBuildConfig config = SolrBuildConfig.loadConfig(argv);
        
        DeleteFromSolr dfs = new DeleteFromSolr();        
        dfs.doTheDelete(config);        
    }
    
    public void doTheDelete(SolrBuildConfig config) throws Exception  {
            
            
        DavService davService = DavServiceFactory.getDavService( config );
        
        //  use the most current delete file        
        String prefix = "bibListForDelete"; 
        String deletesDir = config.getWebdavBaseUrl() + "/" + config.getDailyBibDeletes(); 
        String deleteFileURL="notYetSet?";
        try {
            deleteFileURL = FileNameUtils.findMostRecentFile(davService, deletesDir, prefix);
        } catch (Exception e) {                        
            throw new Exception("No documents have been deleted, could not find the most recent deletes "
                    + "file from " + deletesDir + " with prefix " + prefix, e );
        }            
        if( deleteFileURL == null ) {
            System.out.println("No documents have been deleted, could not find the most recent deletes "
                    + "file from " + deletesDir + " with prefix " + prefix);
            return;
        }
        
        String solrURL = config.getSolrUrl();                                        
        SolrServer solr = new HttpSolrServer( solrURL );        
                        
        int lineNum = 0;
        try{
            System.out.println("Deleteing BIB IDs found in: " + deleteFileURL);
            System.out.println("from Solr at: " + solrURL);
    
            long countBeforeDel = countOfDocsInSolr( solr );
            InputStream is = davService.getFileAsInputStream( deleteFileURL );      
            BufferedReader reader = new BufferedReader(new InputStreamReader( is , "UTF-8" ));
                
            int batchSize = 1000;
            List<String> ids = new ArrayList<String>(batchSize);
            
            int commitSize = batchSize * 10;
            
            String line;   
            while ((line = reader.readLine()) != null)   {
                if( line != null && !line.trim().isEmpty()){
                    lineNum++;
                    ids.add( line );
                }
                
                if( ids.size() >= batchSize ){
                    solr.deleteById( ids );
                    ids.clear();                    
                }                    
                
                if( lineNum % commitSize == 0 ){
                    System.out.println("Requested " + lineNum + " deletes and doing a commit.");
                    solr.commit();
                }                
            }    

            if( ids.size() > 0 ){
                solr.deleteById( ids );
            }

            System.out.println("Doing end of batch commit and reopening Solr server's searchers.");
            solr.commit(true,true,true);
            
            long countAfterDel = countOfDocsInSolr( solr );
            
            System.out.println("Expeced to delete " + lineNum + " documents from Solr index.");
            System.out.println("Solr document count before delete: " + countBeforeDel + 
                    " count after: " + countAfterDel + " difference: " + (countBeforeDel - countAfterDel));            
            
        } catch (Exception e) {
            throw new Exception( "Exception while processing deletes form file " + deleteFileURL + 
                    ", problem around line " + lineNum + ", some documents may "
                    + "have been deleted from Solr.", e);
        }
        
        System.out.println("Success: requested " + lineNum + " documents "
                + "to be deleted from solr at " + config.getSolrUrl());
    }    
    
    private static long countOfDocsInSolr( SolrServer solr ) throws SolrServerException{
        SolrQuery query = new SolrQuery();
        query.set("qt", "standard");
        query.setQuery("*:*");
        query.setRows(0);
        return solr.query( query ).getResults().getNumFound();        
    }
}
