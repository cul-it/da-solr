package edu.cornell.library.integration.delete;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilies.IndexingUtilities;
import edu.cornell.library.integration.ilcommons.service.DavService;

/**
 * Utility to delete bibs from Solr index.
 * 
 * Expects a URL of a file that it can get via WEBDAV. If the file 
 * is not provided as an argument to main, then the most current
 * delete file will be used.
 * 
 * The file should have a single Bib ID per a line. It should be UTF-8 encoded.
 * 
 * A delete requests will be sent to the Solr service for each bib ID. They
 * may be sent in batches.
 * 
 * @author bdc34
 *
 */
public class DeleteFromSolr {

    String solrURL;
    SolrServer solr;    
    
    String davBaseURL = "http://culdata.library.cornell.edu/data";
    
    public void main(String[] argv)  {
        
        if( argv.length < 1 || argv.length > 2 )
            help();
        
        String solrServiceURL = argv[0];
        solr = new HttpSolrServer( solrServiceURL );        
        
        DavService davService = DavServiceFactory.getDavService();
        
        String deleteFileURL= null;        
        if( argv.length == 2 )
            deleteFileURL= argv[1];        
        else{
            // no delete file specified on cmd line, use the most current delete file
            String dir = davBaseURL + "/updates/bib.updates/";
            String prefix = "bibListForUpdate";                  
            try {
                deleteFileURL = IndexingUtilities.findMostRecentFile(davService, dir, prefix);
            } catch (Exception e) {
                System.out.println("Could not get the most recent deletes file from " + dir );
                System.out.println(e.getMessage());
                System.exit(1);
            }            
        }        
        
        int lineNum = 0;
        try{
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
            System.out.println("Could not process deletes form file " + deleteFileURL);
            System.out.println("problem around line " + lineNum);
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }
    
    public void help(){
        System.out.println("Deletes a list of bibIDs from the solr index.");
        System.out.println("The file should have one bibID per a line and should be avaiable via WEBDAV.");
        System.out.println("args: solrURL [deleteListFileURL]");
        System.exit(1);
    }
}
