package edu.cornell.library.integration.indexer.queues;

import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToUpdateQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class BatchRecordsForSolrIndex {

    /**
	 * Gets a list of BIB IDs at the top of the queue for indexing, and marks those queue items
	 * as batched. The next request will skip these records.
	 * @param current Open DB Connection to the current records Solr inventory DB.
     * @param b BatchLogic object allowing determination of size and rules for target batch
	 * @throws Exception 
	 * 
	 */
	public static Set<Integer> getBibsToIndex( Connection current, BatchLogic b) throws Exception {

		b.startNewBatch();
		int startingTarget = b.targetBatchSize();
		Set<Integer> addedBibs = new HashSet<>(startingTarget);
		boolean isTestMode = b.isTestMode();

        try (Statement stmt = current.createStatement()) {
        	stmt.executeQuery("LOCK TABLES "+CurrentDBTable.QUEUE+" WRITE, "
        			+CurrentDBTable.QUEUE+" AS q READ, "
        			+CurrentDBTable.BIB_SOLR+" AS s READ, "
        			+CurrentDBTable.BIB_VOY+" AS v READ"); }
        try (PreparedStatement pstmt = current.prepareStatement(
        		" SELECT q.bib_id, cause, priority"
        		+"  FROM "+CurrentDBTable.QUEUE+" AS q"
        		+"  JOIN "+CurrentDBTable.BIB_VOY+" AS v ON q.bib_id = v.bib_id"
        		+" WHERE done_date = 0 AND batched_date = 0 AND active = 1"
        		+" ORDER BY priority"
        		+" LIMIT " + startingTarget*2);
        		ResultSet rs = pstmt.executeQuery()) {
        	final String delete = DataChangeUpdateType.DELETE.toString();

        	while (rs.next() && addedBibs.size() < startingTarget) {
        		if (rs.getString(2).equals(delete))
        			continue;
        		if (! b.addQueuedItemToBatch(rs,addedBibs.size()))
        			continue;
        		int bib_id = rs.getInt(1);
        		addedBibs.add(bib_id);
        	}
        }

        int remainingTarget = b.unqueuedTargetCount(addedBibs.size());
        if (remainingTarget > 0) {
        	int adjustedTotalTarget = addedBibs.size() + remainingTarget;
        	try (PreparedStatement pstmt = current.prepareStatement(
        			"SELECT s.bib_id"
        			+" FROM "+CurrentDBTable.BIB_SOLR+" AS s"
        			+" LEFT JOIN "+CurrentDBTable.QUEUE+" AS q"
        					+ " ON (s.bib_id = q.bib_id AND q.done_date = 0)"
        			+" WHERE active = 1"
        			+" AND queued_date IS NULL"
        			+" ORDER BY index_date"
        			+" LIMIT "+remainingTarget);
        			ResultSet rs = pstmt.executeQuery()) {
        		while (rs.next() && addedBibs.size() < adjustedTotalTarget) {
        			int bib_id = rs.getInt(1);
            		if ( ! addedBibs.contains(bib_id) ) {
            			addedBibs.add(bib_id);
            			if ( ! isTestMode )
            				addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.AGE_IN_SOLR);
            		}      		
        		}
        		
        	}
        }
        if ( ! isTestMode ) {
	        try (PreparedStatement batchStmt = current.prepareStatement(
	        		"UPDATE "+CurrentDBTable.QUEUE
	        		+" SET batched_date = NOW() WHERE bib_id = ? AND done_date = 0 AND batched_date = 0")) {
	        	for (int bib_id : addedBibs) {
	        		batchStmt.setInt(1,bib_id);
	        		batchStmt.addBatch();
	        	}
	        	batchStmt.executeBatch();
	        }
        }
        try (Statement stmt = current.createStatement()) {
        	stmt.executeQuery("UNLOCK TABLES"); }
        return addedBibs;
    }

	public interface BatchLogic {
		public void startNewBatch();
		public int targetBatchSize();
		public boolean addQueuedItemToBatch( ResultSet rs, int currentBatchSize ) throws SQLException;
		public int unqueuedTargetCount( int currentBatchSize );
		public boolean isTestMode();
	}
}
