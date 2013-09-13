package edu.cornell.library.integration.delete;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import edu.cornell.library.integration.service.DavService;
import edu.cornell.library.integration.service.DavServiceImpl;

/**
 * Utility to delete bibs from solr index.
 * 
 * Expects a URL of a file that it can get via WEBDAV.
 * The file should have a single Bib ID per a line.
 * It should be UTF-8 encoded.
 * A delete request will be sent to solr for each bib ID.
 * 
 * @author bdc34
 *
 */
public class DeleteFromSolr {

    String solrURL;
    SolrServer solr;
    DavService davService;
    
    public void main(String[] argv) {
        
        String deleteFileURL= argv[0];
        String solrServiceURL = argv[1];
                        
        solr = new HttpSolrServer( solrServiceURL );
        
        String davUser = "admin";
        String davPass = "password";
        
        davService = new DavServiceImpl( davUser, davPass );
        
        try{
            InputStream is = davService.getFileAsInputStream( deleteFileURL );      
            BufferedReader reader = new BufferedReader(new InputStreamReader( is , "UTF-8" ));
    
            List<String> ids = new ArrayList<String>(100);
            
            String line;   
            while ((line = reader.readLine()) != null)   {
                if( line != null && !line.trim().isEmpty()){
                    ids.add( line );
                }
                
                if( ids.size() > 99 ){
                    solr.deleteById(ids, 1000 * 10 );
                    ids.clear();
                }
                    
            }                        
        } catch (Exception e) {
            System.exit(1);
        }
    }    
}
