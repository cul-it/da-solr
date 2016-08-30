package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

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

        System.out.println("Deleting BIB IDs found in queue with: "
        		+DataChangeUpdateType.DELETE);
        System.out.println("from Solr at: " + solrURL);

        try (   SolrClient solr = new HttpSolrClient( solrURL );
        		Connection conn = config.getDatabaseConnection("Current")  ){

        	long countBeforeDel = countOfDocsInSolr( solr );

        	Set<Integer> deleteQueue = new HashSet<Integer>();
        	final String getQueuedQuery =
        			"SELECT bib_id FROM "+CurrentDBTable.QUEUE
        			+" WHERE done_date = 0 AND priority = 0 and batched_date = 0 "
        			+" AND cause = ?";
        	try (  PreparedStatement deleteQueueStmt = conn.prepareStatement(getQueuedQuery) ) {
        		deleteQueueStmt.setString(1,DataChangeUpdateType.DELETE.toString());
        		try (  ResultSet deleteQueueRS = deleteQueueStmt.executeQuery()  ) {

        			while (deleteQueueRS.next())
        				deleteQueue.add(deleteQueueRS.getInt(1));
        		}
        	}

        	if ( deleteQueue.isEmpty() ) {
        		System.out.println("No record deletes were queued.");
        		return;
        	}

        	conn.setAutoCommit(false);

        	System.out.println("Expected to delete " + deleteQueue.size() + " documents from Solr index.");
    		Set<Integer> knockOnUpdates = new HashSet<Integer>();
        	processDeleteQueue(deleteQueue,solr,conn,knockOnUpdates);

        	System.out.println("Doing end of batch commit and reopening Solr server's searchers.");
        	solr.commit(true,true,true);
        	long countAfterDel = countOfDocsInSolr( solr );

        	System.out.println("Solr document count before delete: " + countBeforeDel +
        			" count after: " + countAfterDel + " difference: " + (countBeforeDel - countAfterDel));

        	if ( ! knockOnUpdates.isEmpty()) {
        		System.out.println(String.valueOf(knockOnUpdates.size())
        				+" documents identified as needing update because they share a work_id with deleted rec(s).");
        		final String markBibForUpdateQuery =
        				"INSERT INTO "+CurrentDBTable.QUEUE
        				+ " (bib_id, priority, cause) VALUES"
            			+ " (?, 0, '"+DataChangeUpdateType.TITLELINK+"')";
        		try (  PreparedStatement markBibForUpdateStmt = conn.prepareStatement(markBibForUpdateQuery)  ){

        			for (int bib_id : knockOnUpdates) {
        				markBibForUpdateStmt.setInt(1,bib_id);
        				markBibForUpdateStmt.addBatch();
        			}
        			markBibForUpdateStmt.executeBatch();
        		}
        	}

        	conn.commit();
        	System.out.println("Success: requested " + deleteQueue.size() + " documents "
        			+ "to be deleted from solr at " + config.getSolrUrl());
        } // conn.close() solr.close()
    }

    private static int processDeleteQueue(Set<Integer> deleteQueue,SolrClient solr, Connection conn,
    		Set<Integer> knockOnUpdates) throws SQLException, SolrServerException, IOException {


		int batchSize = 1000;

		int lineNum = 0;
		int commitSize = batchSize * 10;
		List<String> ids = new ArrayList<String>(batchSize);

    	
       	final String bibQuery =
    			"UPDATE "+CurrentDBTable.BIB_SOLR+
    			" SET active = 0, linking_mod_date = NOW() WHERE bib_id = ?";
    	final String markDoneInQueueQuery =
    			"UPDATE "+CurrentDBTable.QUEUE+" SET done_date = NOW()"
    					+ " WHERE bib_id = ? AND done_date = 0";
    	final String workQuery =
    			"UPDATE "+CurrentDBTable.BIB2WORK+
    			" SET active = 0, mod_date = NOW() WHERE bib_id = ?";
    	final String mfhdQuery =
    			"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_SOLR+" WHERE bib_id = ?";
    	final String mfhdDelQuery =
    			"DELETE FROM "+CurrentDBTable.MFHD_SOLR+" WHERE bib_id = ?";
    	final String itemQuery =
    			"DELETE FROM "+CurrentDBTable.ITEM_SOLR+" WHERE mfhd_id = ?";
    	final String knockOnUpdateQuery =
    			"SELECT b.bib_id"
        			+ " FROM "+CurrentDBTable.BIB2WORK+" AS a, "
            				+CurrentDBTable.BIB2WORK+" AS b "
            		+ "WHERE b.work_id = a.work_id"
            		+ " AND a.bib_id = ?"
            		+ " AND a.bib_id != b.bib_id"
            		+ " AND a.active = 1"
            		+ " AND b.active = 1";
    	try (   PreparedStatement bibStmt = conn.prepareStatement(bibQuery);
    			PreparedStatement markDoneInQueueStmt = conn.prepareStatement(markDoneInQueueQuery);
    			PreparedStatement workStmt = conn.prepareStatement(workQuery);
    			PreparedStatement mfhdStmt = conn.prepareStatement(mfhdQuery);
    			PreparedStatement mfhdDelStmt = conn.prepareStatement(mfhdDelQuery);
    			PreparedStatement itemStmt = conn.prepareStatement(itemQuery);
    			PreparedStatement knockOnUpdateStmt = conn.prepareStatement(knockOnUpdateQuery)  ){

    		for (int bib_id : deleteQueue)  {
			if (bib_id == 0)
				continue;
			lineNum++;
			ids.add( String.valueOf(bib_id) );
			knockOnUpdateStmt.setInt(1,bib_id);
			try (  ResultSet rs = knockOnUpdateStmt.executeQuery()  ){

				while (rs.next()) {
					int other_bib_id = rs.getInt(1);
					if ( ! ids.contains( String.valueOf(other_bib_id) ))
						knockOnUpdates.add(other_bib_id);
				}
			}
			knockOnUpdates.remove(bib_id);
			bibStmt.setInt(1,bib_id);
			bibStmt.addBatch();
			markDoneInQueueStmt.setInt(1,bib_id);
			markDoneInQueueStmt.addBatch();
			workStmt.setInt(1,bib_id);
			workStmt.addBatch();
			mfhdStmt.setInt(1,bib_id);
			try ( ResultSet rs = mfhdStmt.executeQuery() ) {

				while (rs.next()) {
					itemStmt.setInt(1,rs.getInt(1));
					itemStmt.addBatch();
				}
			}
			mfhdDelStmt.setInt(1,bib_id);
			mfhdDelStmt.addBatch();

			if( ids.size() >= batchSize ){
				pushUpdates(solr,ids,bibStmt,markDoneInQueueStmt,workStmt,mfhdDelStmt,itemStmt);
				ids.clear();
			}

			if( lineNum % commitSize == 0 ){
				System.out.println("Requested " + lineNum + " deletes and doing a commit.");
				solr.commit();
				conn.commit();
			}
		}
		if( ids.size() > 0 )
			pushUpdates(solr,ids,bibStmt,markDoneInQueueStmt,workStmt,mfhdDelStmt,itemStmt);

    	} // preparedStatements.close()
		return lineNum;
    	
    }
    private static void pushUpdates(SolrClient solr, List<String> ids, PreparedStatement bibStmt,
    		PreparedStatement markDoneInQueueStmt, PreparedStatement workStmt,
    		PreparedStatement mfhdDelStmt, PreparedStatement itemStmt)
    				throws SolrServerException, IOException, SQLException {
		solr.deleteById( ids );
		bibStmt.executeBatch();
		markDoneInQueueStmt.executeBatch();
		workStmt.executeBatch();
		mfhdDelStmt.executeBatch();
		itemStmt.executeBatch();
    }
    private static long countOfDocsInSolr( SolrClient solr ) throws SolrServerException, IOException {
        SolrQuery query = new SolrQuery();
        query.set("qt", "standard");
        query.setQuery("*:*");
        query.setRows(0);
        return solr.query( query ).getResults().getNumFound();
    }
}
