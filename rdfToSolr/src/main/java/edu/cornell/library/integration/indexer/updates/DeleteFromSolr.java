package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.utilities.FileNameUtils;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

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

		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.add("solrUrl");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("dailyBibDeletes");
		requiredArgs.add("dailyBibUpdates");

        SolrBuildConfig config = SolrBuildConfig.loadConfig(argv,requiredArgs);
        
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

		Set<Integer> knockOnUpdates = new HashSet<Integer>();
                        
        int lineNum = 0;
        Connection conn = null;
        try{
            System.out.println("Deleteing BIB IDs found in: " + deleteFileURL);
            System.out.println("from Solr at: " + solrURL);

            conn = config.getDatabaseConnection("Current");
            conn.setAutoCommit(false);

            long countBeforeDel = countOfDocsInSolr( solr );
            InputStream is = davService.getFileAsInputStream( deleteFileURL );      
            BufferedReader reader = new BufferedReader(new InputStreamReader( is , "UTF-8" ));
                
            int batchSize = 1000;
            List<String> ids = new ArrayList<String>(batchSize);
            
            int commitSize = batchSize * 10;

            PreparedStatement bibStmt = conn.prepareStatement(
            		"UPDATE "+CurrentDBTable.BIB_SOLR.toString()+
            		" SET active = 0, linking_mod_date = NOW() WHERE bib_id = ?");
    		PreparedStatement markDoneInQueueStmt = conn.prepareStatement(
    				"UPDATE "+CurrentDBTable.QUEUE.toString()+" SET done_date = NOW()"
    						+ " WHERE bib_id = ? AND NOT done_date");
            PreparedStatement workStmt = conn.prepareStatement(
            		"UPDATE "+CurrentDBTable.BIB2WORK.toString()+
            		" SET active = 0, mod_date = NOW() WHERE bib_id = ?");
            PreparedStatement dequeueStmt = conn.prepareStatement(
            		"DELETE FROM "+CurrentDBTable.QUEUE.toString()+
            		" WHERE bib_id = ? AND NOT done_date");
            PreparedStatement mfhdQueryStmt = conn.prepareStatement(
            		"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_SOLR.toString()+
            		" WHERE bib_id = ?");
            PreparedStatement mfhdDelStmt = conn.prepareStatement(
            		"DELETE FROM "+CurrentDBTable.MFHD_SOLR.toString()+
            		" WHERE bib_id = ?");
            PreparedStatement itemStmt = conn.prepareStatement(
            		"DELETE FROM "+CurrentDBTable.ITEM_SOLR.toString()+
            		" WHERE mfhd_id = ?");
            PreparedStatement knockOnUpdateStmt = conn.prepareStatement(
            		"SELECT b.bib_id"
            		+ " FROM "+CurrentDBTable.BIB2WORK.toString()+" AS a, "
            				+CurrentDBTable.BIB2WORK.toString()+" AS b "
            		+ "WHERE b.work_id = a.work_id"
            		+ " AND a.bib_id = ?"
            		+ " AND a.bib_id != b.bib_id"
            		+ " AND a.active = 1"
            		+ " AND b.active = 1");

            String line;   
            while ((line = reader.readLine()) != null)   {
                if( line != null && !line.trim().isEmpty()){
                    lineNum++;
                    ids.add( line );
                    int id = Integer.valueOf(line);
                    knockOnUpdateStmt.setInt(1,id);
                    ResultSet rs = knockOnUpdateStmt.executeQuery();
                    while (rs.next())
                    	knockOnUpdates.add(rs.getInt(1));
                    rs.close();
                    knockOnUpdates.remove(id);
                    bibStmt.setInt(1,id);
                    bibStmt.addBatch();
                    dequeueStmt.setInt(1,id);
                    dequeueStmt.addBatch();
                    markDoneInQueueStmt.setInt(1,id);
                    markDoneInQueueStmt.addBatch();
                    workStmt.setInt(1,id);
                    workStmt.addBatch();
                    mfhdQueryStmt.setInt(1,id);
                    rs = mfhdQueryStmt.executeQuery();
                    while (rs.next()) {
                    	itemStmt.setInt(1,rs.getInt(1));
                    	itemStmt.addBatch();
                    }
                    rs.close();
                    mfhdDelStmt.setInt(1,id);
                    mfhdDelStmt.addBatch();
                }
                
                if( ids.size() >= batchSize ){
                    solr.deleteById( ids );
                    bibStmt.executeBatch();
                    dequeueStmt.executeBatch();
                    markDoneInQueueStmt.executeBatch();
                    workStmt.executeBatch();
                    mfhdDelStmt.executeBatch();
                    itemStmt.executeBatch();
                    ids.clear();                    
                }                    
                
                if( lineNum % commitSize == 0 ){
                    System.out.println("Requested " + lineNum + " deletes and doing a commit.");
                    solr.commit();
                    conn.commit();
                }                
            }    

            if( ids.size() > 0 ){
                solr.deleteById( ids );
                bibStmt.executeBatch();
                dequeueStmt.executeBatch();
                markDoneInQueueStmt.executeBatch();
                workStmt.executeBatch();
                mfhdDelStmt.executeBatch();
                itemStmt.executeBatch();
            }

            System.out.println("Doing end of batch commit and reopening Solr server's searchers.");
            solr.commit(true,true,true);
            conn.commit();
            
            long countAfterDel = countOfDocsInSolr( solr );
            
            System.out.println("Expected to delete " + lineNum + " documents from Solr index.");
            System.out.println("Solr document count before delete: " + countBeforeDel + 
                    " count after: " + countAfterDel + " difference: " + (countBeforeDel - countAfterDel));
            
            if ( ! knockOnUpdates.isEmpty()) {
            	System.out.println(String.valueOf(knockOnUpdates.size())
            			+" documents identified as needing update because they share a work_id with deleted rec(s).");
            	produceUpdateFile(config,davService,knockOnUpdates);
            }
            
        } catch (Exception e) {
            throw new Exception( "Exception while processing deletes form file " + deleteFileURL + 
                    ", problem around line " + lineNum + ", some documents may "
                    + "have been deleted from Solr.", e);
        } finally {
        	conn.close();
        }
        
        System.out.println("Success: requested " + lineNum + " documents "
                + "to be deleted from solr at " + config.getSolrUrl());
    }

	private void produceUpdateFile( SolrBuildConfig config,
			DavService davService, Set<Integer> bibsToUpdate) throws Exception {

		if (bibsToUpdate != null && bibsToUpdate.size() > 0){			
			
			Integer[] arr = bibsToUpdate.toArray(new Integer[ bibsToUpdate.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
				sb.append(id);
				sb.append("\n");
			}

			String updateReport = sb.toString();

			String fileName = config.getWebdavBaseUrl() + "/" + config.getDailyBibUpdates() + "/"
			        + "deletedRecordKnockOnBibList.txt";
			try {			    
				davService.saveFile(fileName, new ByteArrayInputStream(updateReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + fileName);
			} catch (Exception e) {
			    throw new Exception("Could not save list of "
			            + "BIB IDs that need update to file '" + fileName + "'",e);   
			}
		}
	}

    private static long countOfDocsInSolr( SolrServer solr ) throws SolrServerException{
        SolrQuery query = new SolrQuery();
        query.set("qt", "standard");
        query.setQuery("*:*");
        query.setRows(0);
        return solr.query( query ).getResults().getNumFound();        
    }
}
