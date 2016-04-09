package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;

import java.io.ByteArrayInputStream;
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
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import edu.cornell.library.integration.utilities.FileNameUtils;

/**
 * Utility to delete bibs from Solr index.
 * 
 * Identifies bibs that should be deleted from solr based on the "Current"
 * database's CurrentDBTable.QUEUE table entries with `cause` listed as
 * DataChangeUpdateType.DELETE.
 * 
 * Bibs are deleted from Solr, and the "Current" database is updated to reflect
 * changes in the current Solr contents. Finally, any bibs that might link to
 * the deleted bibs based on shared work id's will be queued to be refreshed
 * in Solr by adding entries to the CurrentDBTable.QUEUE with
 * DataChangeUpdateType.TITLELINK.
 */
public class DeleteFromSolr {
           
    public static void main(String[] argv) throws Exception{

		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.add("solrUrl");

        SolrBuildConfig config = SolrBuildConfig.loadConfig(argv,requiredArgs);
        
        DeleteFromSolr dfs = new DeleteFromSolr();        
        dfs.doTheDelete(config);        
    }
    
    public void doTheDelete(SolrBuildConfig config) throws Exception  {

        String solrURL = config.getSolrUrl();                                        
        SolrServer solr = new HttpSolrServer( solrURL );

		Set<Integer> knockOnUpdates = new HashSet<Integer>();

        int lineNum = 0;
        Connection conn = null;
        try{
            System.out.println("Deleting BIB IDs found in queue with: "
            		+DataChangeUpdateType.DELETE.toString());
            System.out.println("from Solr at: " + solrURL);

            config.setDatabasePoolsize("Current", 2);
            conn = config.getDatabaseConnection("Current");
            conn.setAutoCommit(false);

            long countBeforeDel = countOfDocsInSolr( solr );

            Connection getQueuedConn = config.getDatabaseConnection("Current");
            PreparedStatement deleteQueueStmt = getQueuedConn.prepareStatement(
            		"SELECT bib_id FROM "+CurrentDBTable.QUEUE.toString()
            		+" WHERE done_date = 0 AND priority = 0"
            		+" AND cause = ?");
            deleteQueueStmt.setString(1,DataChangeUpdateType.DELETE.toString());
            ResultSet deleteQueueRS = deleteQueueStmt.executeQuery();
            int batchSize = 1000;
            List<String> ids = new ArrayList<String>(batchSize);

            int commitSize = batchSize * 10;

            PreparedStatement bibStmt = conn.prepareStatement(
            		"UPDATE "+CurrentDBTable.BIB_SOLR.toString()+
            		" SET active = 0, linking_mod_date = NOW() WHERE bib_id = ?");
    		PreparedStatement markDoneInQueueStmt = conn.prepareStatement(
    				"UPDATE "+CurrentDBTable.QUEUE.toString()+" SET done_date = NOW()"
    						+ " WHERE bib_id = ? AND done_date = 0");
            PreparedStatement workStmt = conn.prepareStatement(
            		"UPDATE "+CurrentDBTable.BIB2WORK.toString()+
            		" SET active = 0, mod_date = NOW() WHERE bib_id = ?");
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

            while (deleteQueueRS.next())   {
            	int bib_id = deleteQueueRS.getInt(1);
            	if (bib_id == 0)
            		continue;	
            	lineNum++;
            	ids.add( String.valueOf(bib_id) );
            	knockOnUpdateStmt.setInt(1,bib_id);
            	ResultSet rs = knockOnUpdateStmt.executeQuery();
            	while (rs.next())
            		knockOnUpdates.add(rs.getInt(1));
            	rs.close();
            	knockOnUpdates.remove(bib_id);
            	bibStmt.setInt(1,bib_id);
            	bibStmt.addBatch();
            	markDoneInQueueStmt.setInt(1,bib_id);
            	markDoneInQueueStmt.addBatch();
            	workStmt.setInt(1,bib_id);
            	workStmt.addBatch();
            	mfhdQueryStmt.setInt(1,bib_id);
            	rs = mfhdQueryStmt.executeQuery();
            	while (rs.next()) {
            		itemStmt.setInt(1,rs.getInt(1));
            		itemStmt.addBatch();
            	}
            	rs.close();
            	mfhdDelStmt.setInt(1,bib_id);
            	mfhdDelStmt.addBatch();
                
            	if( ids.size() >= batchSize ){
            		solr.deleteById( ids );
            		bibStmt.executeBatch();
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
            deleteQueueStmt.close();
            getQueuedConn.close();

            if( ids.size() > 0 ){
                solr.deleteById( ids );
                bibStmt.executeBatch();
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
            	PreparedStatement markBibForUpdateStmt = conn.prepareStatement(
        				"INSERT INTO "+CurrentDBTable.QUEUE.toString()
        				+ " (bib_id, priority, cause) VALUES"
        				+ " (?, 0, '"+DataChangeUpdateType.TITLELINK.toString()+"')");
            	for (int bib_id : knockOnUpdates) {
            		markBibForUpdateStmt.setInt(1,bib_id);
            		markBibForUpdateStmt.addBatch();
            	}
            	markBibForUpdateStmt.executeBatch();
            	markBibForUpdateStmt.close();
            }
            
        } catch (Exception e) {
        	throw new Exception( "Exception while processing deletes, some documents may "
        			+ "have been deleted from Solr.", e);
        } finally {
        	conn.close();
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
