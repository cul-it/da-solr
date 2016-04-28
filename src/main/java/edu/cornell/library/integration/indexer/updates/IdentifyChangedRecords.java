package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison.ChangedBib;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import static edu.cornell.library.integration.utilities.IndexingUtilities.queueBibDelete;

/**
 * Identify record changes from Voyager. This is done by comparing the
 * list of active BIB, MFHD and item records in Voyager along with their
 * modification dates with the similar lists in the Solr index.<br/><br/>
 *   
 * Bibs should be updated in Solr if any of the following are true:<br/>
 *  1. The set of mfhd records associated with the bib does not match.<br/>
 *  2. The set of item records associated with EACH mfhd does not match.<br/>
 *       (One possible modification involves the reassignment of an item
 *        between two mfhds for the same bib.)<br/>
 *  4. The Voyager modification dates for any single bib, mfhd or item<br/>
 *     record related to a bib is more recent than the dates in Solr.<br/><br/>
 *  
 * The reasons for deleting or adding a bib in Solr should be obvious,
 * though it's worth noting that for both bibs and holdings records, a 
 * suppressed record in Voyager is functionally equivalent to an absent
 * one.<br/><br/>
 * 
 * The lists are generated by comparing lists from step 1 (which is
 * edu.cornell.library.integration.indexer.updates.IdentifyCurrentVoyagerRecords)
 * with the contents of the Solr index. This step doesn't need to run
 * immediately after step 1, but a long delay may result in records
 * available during step 1 being unavailable during indexing, which
 * may cause the post-update index integrity check to report failure.
 */
public class IdentifyChangedRecords {
	
	DavService davService;
	SolrBuildConfig config;
	String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
	Set<Integer> updatedBibs = new HashSet<Integer>();
	PreparedStatement bibQueueStmt = null;
	private static Timestamp max_date = null;

	public static void main(String[] args)  {
		boolean thorough = true;
		if (args.length > 0)
			thorough = Boolean.valueOf(args[0]);

		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Current"));
		if (thorough)
			requiredArgs.addAll(IndexRecordListComparison.requiredArgs());
		else
			requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));

		try{        
			new IdentifyChangedRecords( SolrBuildConfig.loadConfig( null, requiredArgs ),thorough);
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IdentifyChangedRecords(SolrBuildConfig config, Boolean thorough) throws Exception {
		this.config = config;
		if (thorough) {
			System.out.println("Launching thorough check for Voyager record changes.");
			thoroughIdentifiationOfChanges();
		} else {
			System.out.println("Launching quick check for Voyager record changes.");
			quickIdentificationOfChanges();
		}
	}

	private void quickIdentificationOfChanges() throws Exception {
		updatedBibs.clear();
		Connection current = config.getDatabaseConnection("Current");
		bibQueueStmt = current.prepareStatement(
				"INSERT INTO "+CurrentDBTable.QUEUE
				+" (bib_id, priority, cause)"
				+" VALUES (?, 0, ?)");

		Statement stmtCurrent = current.createStatement();
		ResultSet rs = null;
		if (max_date == null)
			rs = stmtCurrent.executeQuery("SELECT max(bib_id), max(record_date) FROM "+CurrentDBTable.BIB_VOY);
		else
			rs = stmtCurrent.executeQuery("SELECT max(bib_id) FROM "+CurrentDBTable.BIB_VOY);
		Timestamp ts = null;
		Integer max_bib = 0, max_mfhd = 0, max_item = 0;
		while (rs.next()) {
			if (max_date == null)
				max_date = rs.getTimestamp(2);
			ts = max_date;
			max_bib = rs.getInt(1);
		}
		ts.setTime(ts.getTime() - (120/*seconds*/
				                    * 1000/*millis per second*/));
		rs.close();
		rs = stmtCurrent.executeQuery("SELECT max(mfhd_id) FROM "+CurrentDBTable.MFHD_VOY);
		while (rs.next())
			max_mfhd = rs.getInt(1);
		rs.close();
		rs = stmtCurrent.executeQuery("SELECT max(item_id) FROM "+CurrentDBTable.ITEM_VOY);
		while (rs.next())
			max_item = rs.getInt(1);
		rs.close();
		
		Connection voyager = config.getDatabaseConnection("Voy");
		PreparedStatement pstmt = voyager.prepareStatement(
				"select BIB_ID, UPDATE_DATE, SUPPRESS_IN_OPAC from BIB_MASTER"
				+ " where ( BIB_ID > ? or UPDATE_DATE > ?)");
		pstmt.setInt(1, max_bib);
		pstmt.setTimestamp(2, ts);
		rs = pstmt.executeQuery();
		while (rs.next()) {
			Timestamp thisTS = rs.getTimestamp(2);
			String suppress_in_opac = rs.getString(3);
			int bib_id = rs.getInt(1);
			if (updatedBibs.contains(bib_id))
				continue;
			if (suppress_in_opac != null && suppress_in_opac.equals("N"))
				queueBib( current, bib_id, thisTS );
			else
				queueBibDelete( current, bib_id );
			updatedBibs.add(bib_id);
			if (thisTS != null && 0 > thisTS.compareTo(max_date))
				max_date = thisTS;
		}
		rs.close();
		pstmt.close();

		int bibCount = updatedBibs.size();
		System.out.println("Queued from poling bib data: "+bibCount);

		pstmt = voyager.prepareStatement(
				"select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, UPDATE_DATE"
	    		 +"  from BIB_MFHD, MFHD_MASTER"
	             +" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
	             + "  and SUPPRESS_IN_OPAC = 'N'"
	             + " and ( MFHD_MASTER.MFHD_ID > ? or UPDATE_DATE > ?)");
		pstmt.setInt(1, max_mfhd);
		pstmt.setTimestamp(2, ts);
		rs = pstmt.executeQuery();
		while (rs.next())
			queueMfhd( current, rs.getInt(1), rs.getInt(2), rs.getTimestamp(3));
		rs.close();
		pstmt.close();

		int mfhdCount = updatedBibs.size() - bibCount;
		System.out.println("Queued from poling holdings data: "+mfhdCount);

		pstmt = voyager.prepareStatement(
				"select MFHD_ITEM.MFHD_ID, ITEM.ITEM_ID, ITEM.MODIFY_DATE"
	    		+"  from MFHD_ITEM, ITEM"
	    		+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID"
	    		+" and ( ITEM.ITEM_ID > ? or MODIFY_DATE > ?)");
		pstmt.setInt(1, max_item);
		pstmt.setTimestamp(2, ts);
		rs = pstmt.executeQuery();
		while (rs.next())
			queueItem( current, rs.getInt(1), rs.getInt(2), rs.getTimestamp(3));
		rs.close();
		pstmt.close();

		int itemCount = updatedBibs.size() - bibCount - mfhdCount;
		System.out.println("Queued from poling item data: "+itemCount);
		System.out.println("Total bibs queued: "+updatedBibs.size());
		bibQueueStmt.close();
		current.close();
		voyager.close();
	}

	private void addBibToIndexQueue(Connection current, Integer bib_id, DataChangeUpdateType reason) throws SQLException {
		bibQueueStmt.setInt(1, bib_id);
		bibQueueStmt.setString(2,reason.toString());
		bibQueueStmt.executeUpdate();
	}


	private void queueBib(Connection current, int bib_id, Timestamp update_date) throws SQLException {
		PreparedStatement bibVoyQStmt = current.prepareStatement(
					"SELECT record_date FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?");
		bibVoyQStmt.setInt(1, bib_id);
		ResultSet rs = bibVoyQStmt.executeQuery();
		while (rs.next()) {
			Timestamp old_date = rs.getTimestamp(1);
			if (update_date != null
					&& (old_date == null
					    || 0 > old_date.compareTo(update_date))) {
				// bib is already in current, but has been updated
				PreparedStatement bibVoyUStmt = current.prepareStatement(
							"UPDATE "+CurrentDBTable.BIB_VOY
							+" SET record_date = ?"
							+" WHERE bib_id = ?");
				bibVoyUStmt.setTimestamp(1, update_date);
				bibVoyUStmt.setInt(2, bib_id);
				bibVoyUStmt.executeUpdate();
				bibVoyUStmt.close();
				addBibToIndexQueue(current, bib_id, DataChangeUpdateType.BIB_UPDATE);
			} // else bib is unchanged - do nothing
			rs.close();
			bibVoyQStmt.close();
			return;
		}
		rs.close();
		bibVoyQStmt.close();

		// bib is not yet in current db
		PreparedStatement bibVoyIStmt = current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.BIB_VOY
					+" (bib_id, record_date) VALUES (?, ?)");
		bibVoyIStmt.setInt(1, bib_id);
		bibVoyIStmt.setTimestamp(2, update_date);
		bibVoyIStmt.executeUpdate();
		bibVoyIStmt.close();
		addBibToIndexQueue(current, bib_id, DataChangeUpdateType.ADD);
	}

	private void queueMfhd(Connection current, int bib_id, int mfhd_id,
			Timestamp update_date) throws SQLException {
		if ( ! isBibActive(current,bib_id))
			return;

		PreparedStatement mfhdVoyQStmt = prepareMfhdVoyQStmt(current);
		mfhdVoyQStmt.setInt(1, mfhd_id);
		ResultSet rs = mfhdVoyQStmt.executeQuery();
		while (rs.next()) {
			Timestamp old_date = rs.getTimestamp(2);
			if (update_date != null
					&& (old_date == null
					    || 0 > old_date.compareTo(update_date))) {
				// mfhd is already in current, but has been updated
				int old_bib = rs.getInt(1);
				PreparedStatement mfhdVoyUStmt = current.prepareStatement(
							"UPDATE "+CurrentDBTable.MFHD_VOY
							+" SET record_date = ?, bib_id = ?"
							+" WHERE mfhd_id = ?");
				mfhdVoyUStmt.setTimestamp(1, update_date);
				mfhdVoyUStmt.setInt(2, bib_id);
				mfhdVoyUStmt.setInt(3, mfhd_id);
				mfhdVoyUStmt.executeUpdate();
				mfhdVoyUStmt.close();
				if (! updatedBibs.contains(bib_id)) {
					addBibToIndexQueue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE);
					updatedBibs.add(bib_id);
				}

				if (old_bib != bib_id
						&& ! updatedBibs.contains(old_bib)) {
					addBibToIndexQueue(current, old_bib, DataChangeUpdateType.MFHD_UPDATE);
					updatedBibs.add(old_bib);
				}
			} // else mfhd is unchanged - do nothing
			rs.close();
			mfhdVoyQStmt.close();
			return;
		}
		rs.close();
		mfhdVoyQStmt.close();

		// mfhd is not yet in current db
		PreparedStatement mfhdVoyIStmt = current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.MFHD_VOY
					+" (bib_id, mfhd_id, record_date)"
					+" VALUES (?, ?, ?)");
		mfhdVoyIStmt.setInt(1, bib_id);
		mfhdVoyIStmt.setInt(2, mfhd_id);
		mfhdVoyIStmt.setTimestamp(3, update_date);
		mfhdVoyIStmt.executeUpdate();
		mfhdVoyIStmt.close();
		if (! updatedBibs.contains(bib_id)) {
			addBibToIndexQueue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE);
			updatedBibs.add(bib_id);
		}
	}

	private void queueItem(Connection current, int mfhd_id, int item_id,
			Timestamp update_date) throws SQLException {
		PreparedStatement itemVoyQStmt = current.prepareStatement(
					"SELECT mfhd_id, record_date"
					+" FROM "+CurrentDBTable.ITEM_VOY
					+" WHERE item_id = ?");
		itemVoyQStmt.setInt(1, item_id);
		ResultSet rs = itemVoyQStmt.executeQuery();
		while (rs.next()) {
			Timestamp old_date = rs.getTimestamp(2);
			if (update_date != null
					&& (old_date == null
					    || 0 > old_date.compareTo(update_date))) {
				// item is already in current, but has been updated
				int old_mfhd = rs.getInt(1);
				PreparedStatement itemVoyUStmt = current.prepareStatement(
							"UPDATE "+CurrentDBTable.ITEM_VOY
							+" SET record_date = ?, mfhd_id = ?"
							+" WHERE item_id = ?");
				itemVoyUStmt.setTimestamp(1, update_date);
				itemVoyUStmt.setInt(2, mfhd_id);
				itemVoyUStmt.setInt(3, item_id);
				itemVoyUStmt.executeUpdate();
				itemVoyUStmt.close();

				int bib_id = getBibIdForMfhd(current, mfhd_id);
				if (bib_id > 0
						&& isBibActive(current,bib_id)
						&& ! updatedBibs.contains(bib_id)) {
					addBibToIndexQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE);
					updatedBibs.add(bib_id);
				}

				if (mfhd_id != old_mfhd) {
					int old_bib_id = getBibIdForMfhd(current, old_mfhd);
					if ( old_bib_id > 0
							&& old_bib_id != bib_id
							&& isBibActive(current,old_bib_id)
							&& ! updatedBibs.contains(old_bib_id)) {
						addBibToIndexQueue(current, old_bib_id, DataChangeUpdateType.ITEM_UPDATE);
						updatedBibs.add(old_bib_id);
					}
				}
			} // else item is unchanged - do nothing
			rs.close();
			itemVoyQStmt.close();
			return;
		}
		rs.close();
		itemVoyQStmt.close();

		// item is not yet in current db
		PreparedStatement itemVoyIStmt = current.prepareStatement(
					"INSERT INTO "+CurrentDBTable.ITEM_VOY
					+" (mfhd_id, item_id, record_date)"
					+" VALUES (?, ?, ?)");
		itemVoyIStmt.setInt(1, mfhd_id);
		itemVoyIStmt.setInt(2, item_id);
		itemVoyIStmt.setTimestamp(3, update_date);
		itemVoyIStmt.executeUpdate();
		itemVoyIStmt.close();
		int bib_id = getBibIdForMfhd(current,mfhd_id);
		if (bib_id > 0
				&& isBibActive(current,bib_id)
				&& ! updatedBibs.contains(bib_id)) {
			addBibToIndexQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE);
			updatedBibs.add(bib_id);
		}
		
	}
	private boolean isBibActive(Connection current, Integer bib_id) throws SQLException {
		PreparedStatement bibVoyQStmt = current.prepareStatement(
				"SELECT record_date FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?");
		bibVoyQStmt.setInt(1, bib_id);
		ResultSet rs = bibVoyQStmt.executeQuery();
		boolean exists = false;
		while (rs.next())
			exists = true;
		rs.close();
		bibVoyQStmt.close();
		return exists;
	}
	private int getBibIdForMfhd(Connection current, Integer mfhd_id) throws SQLException {
		PreparedStatement mfhdVoyQStmt = prepareMfhdVoyQStmt(current);
		mfhdVoyQStmt.setInt(1, mfhd_id);
		ResultSet rs = mfhdVoyQStmt.executeQuery();
		Integer bib_id = 0;
		while (rs.next())
			bib_id = rs.getInt(1);
		rs.close();
		mfhdVoyQStmt.close();
		return bib_id;
	}

	private PreparedStatement prepareMfhdVoyQStmt(Connection current) throws SQLException {
		return current.prepareStatement(
				"SELECT bib_id, record_date"
				+" FROM "+CurrentDBTable.MFHD_VOY
				+" WHERE mfhd_id = ?");
	}

	private void thoroughIdentifiationOfChanges() throws Exception {

	    this.davService = DavServiceFactory.getDavService( config );
		System.out.println("Comparing to contents of index at: " + config.getSolrUrl() );

		IndexRecordListComparison c = new IndexRecordListComparison(config);

		Set<Integer> bibsToAdd = c.bibsInVoyagerNotIndex();
		System.out.println("Bibs To Add to Solr: "+bibsToAdd.size());
		produceAddFile( bibsToAdd );
		Set<Integer> bibsToDelete = c.bibsInIndexNotVoyager();
		System.out.println("Bibs To Delete from Solr: "+bibsToDelete.size());
		produceDeleteFile( bibsToDelete );

		System.out.println("Bibs To Update:");

		Set<Integer> bibsToUpdate = c.bibsNewerInVoyagerThanIndex();
		System.out.println("\tbibsNewerInVoyagerThanIndex: "+bibsToUpdate.size());

		Set<Integer> markedBibs = c.bibsMarkedAsNeedingReindexingDueToDataChange();
		System.out.println("\tbibsMarkedAsNeedingReindexingDueToDataChange: "+markedBibs.size());

		Map<Integer,Integer> tempMap = c.mfhdsNewerInVoyagerThanIndex();
		System.out.println("\tmfhdsNewerInVoyagerThanIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsNewerInVoyagerThanIndex();
		System.out.println("\titemsNewerInVoyagerThanIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsInIndexNotVoyager();
		System.out.println("\tmfhdsInIndexNotVoyager: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsInVoyagerNotIndex();
		System.out.println("\tmfhdsInVoyagerNotIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInIndexNotVoyager();
		System.out.println("\titemsInIndexNotVoyager: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInVoyagerNotIndex();
		System.out.println("\titemsInVoyagerNotIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();
		
		Map<Integer,ChangedBib> tempCBMap = c.mfhdsAttachedToDifferentBibs();
		System.out.println("\tmfhdsAttachedToDifferentBibs: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();

		tempCBMap = c.itemsAttachedToDifferentMfhds();
		System.out.println("\titemsAttachedToDifferentMfhds: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();

		bibsToUpdate.removeAll(bibsToDelete);
		bibsToUpdate.removeAll(bibsToAdd);
		c.queueBibs( bibsToUpdate, DataChangeUpdateType.UPDATE );

		bibsToUpdate.addAll(markedBibs);
		markedBibs.clear();
		bibsToUpdate.removeAll(bibsToDelete);
		bibsToUpdate.removeAll(bibsToAdd);

		System.out.println("Bibs To Update in Solr: "+bibsToUpdate.size());		
		produceUpdateFile(bibsToUpdate);
		c.queueBibs( bibsToDelete, DataChangeUpdateType.DELETE );
		c.queueBibs( bibsToAdd, DataChangeUpdateType.ADD );

		c = null; // to allow GC
	
 	}

	private void produceDeleteFile( Set<Integer> bibsToDelete ) throws Exception {

		// Write a file of BIBIDs that are in the Solr index but not voyager
		if ( bibsToDelete != null && bibsToDelete.size() > 0) {
			Integer[] arr = bibsToDelete.toArray(new Integer[ bibsToDelete.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
				sb.append(id);
				sb.append("\n");
			}

			String deleteReport = sb.toString(); 
			String deleteReportFile = 
			        config.getWebdavBaseUrl() + "/" + config.getDailyBibDeletes() + "/"
			        + "bibListForDelete-"+ currentDate + ".txt";			
			try {
				davService.saveFile( deleteReportFile , new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + deleteReportFile);
			} catch (Exception e) {
				throw new Exception("Could not save report of deletes to '" + deleteReportFile + "'" , e);
			}		
		}
		
	}
	
	private void produceAddFile(Set<Integer> bibsToAdd) throws Exception {

		// Write a file of BIBIDs that are in Voyager but not in the Solr index
		if ( bibsToAdd != null && bibsToAdd.size() > 0) {
			Integer[] arr = bibsToAdd.toArray(new Integer[ bibsToAdd.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
				sb.append(id);
				sb.append("\n");
			}

			String addReport = sb.toString();
			String addReportFile =
			        config.getWebdavBaseUrl() + "/" + config.getDailyBibAdds() + "/"
			        + "bibListToAdd-"+ currentDate + ".txt";
			try {
				davService.saveFile( addReportFile , new ByteArrayInputStream(addReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + addReportFile);
			} catch (Exception e) {
				throw new Exception("Could not save report of adds to '" + addReportFile + "'" , e);
			}
		}

	}

	private void produceUpdateFile( Set<Integer> bibsToUpdate) throws Exception {

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
			        + "bibListForUpdate-"+ currentDate + ".txt";
			try {			    
				davService.saveFile(fileName, new ByteArrayInputStream(updateReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + fileName);
			} catch (Exception e) {
			    throw new Exception("Could not save list of "
			            + "BIB IDs that need update to file '" + fileName + "'",e);   
			}
		}
	}

	public static enum DataChangeUpdateType {
		ADD("Added Record"),
		@Deprecated
		UPDATE("Record Update"),
		BIB_UPDATE("Bibliographic Record Update"),
		MFHD_UPDATE("Holdings Record Change"),
		ITEM_UPDATE("Item Record Change"),
		DELETE("Record Deleted or Suppressed"),
		TITLELINK("Title Link Update");

		private String string;

		private DataChangeUpdateType(String name) {
			string = name;
		}

		public String toString() { return string; }
	}
	
}
