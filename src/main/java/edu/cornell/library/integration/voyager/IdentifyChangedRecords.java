package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToAvailQueue;
import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToUpdateQueue;
import static edu.cornell.library.integration.utilities.IndexingUtilities.queueBibDelete;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.IndexRecordListComparison;
import edu.cornell.library.integration.utilities.IndexRecordListComparison.ChangedBib;

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
	
	private Config config;
	private Set<Integer> updatedBibs = new HashSet<>();
	private static Timestamp max_date = null;

	private final static String maxBibTimestampQuery = "SELECT max(record_date) FROM bibRecsVoyager";
	private final static String recentBibQuery =
			"select BIB_ID, CREATE_DATE, UPDATE_DATE, SUPPRESS_IN_OPAC from BIB_MASTER"
					+" where ( CREATE_DATE > ? or UPDATE_DATE > ?)";
	private final static String recentMfhdQuery =
			"select BIB_MFHD.BIB_ID, MFHD_MASTER.MFHD_ID, CREATE_DATE, UPDATE_DATE, SUPPRESS_IN_OPAC"
					+"  from BIB_MFHD, MFHD_MASTER"
					+" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
					+ "  and ( CREATE_DATE > ? or UPDATE_DATE > ?)";
	private final static String recentItemQuery =
			"select MFHD_ITEM.MFHD_ID, ITEM.ITEM_ID, ITEM.CREATE_DATE, ITEM.MODIFY_DATE"
		    		+"  from MFHD_ITEM, ITEM"
		    		+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID"
		    		+ "  and ( CREATE_DATE > ? or MODIFY_DATE > ?)";
	private final static String bibVoyQuery =
			"SELECT record_date, active FROM bibRecsVoyager WHERE bib_id = ?";
	private final static String bibVoyUpdate =
			"UPDATE bibRecsVoyager SET record_date = ? , active = ? WHERE bib_id = ?";
	private final static String bibVoyInsert =
			"INSERT INTO bibRecsVoyager (bib_id,record_date,active) VALUES (?,?,?)";
	private final static String mfhdVoyQuery =
			"SELECT bib_id, record_date FROM mfhdRecsVoyager WHERE mfhd_id = ?";
	private final static String mfhdVoyDelete =
			"DELETE FROM mfhdRecsVoyager WHERE mfhd_id = ?";
	private final static String mfhdVoyUpdate =
			"UPDATE mfhdRecsVoyager SET record_date = ?, bib_id = ? WHERE mfhd_id = ?";
	private final static String mfhdVoyInsert =
			"INSERT INTO mfhdRecsVoyager (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)";
	private final static String itemVoyQuery =
			"SELECT mfhd_id, record_date FROM itemRecsVoyager WHERE item_id = ?";
	private final static String itemVoyUpdate =
			"UPDATE itemRecsVoyager SET record_date = ?, mfhd_id = ? WHERE item_id = ?";
	private final static String itemVoyInsert =
			"INSERT INTO itemRecsVoyager (mfhd_id, item_id, record_date) VALUES (?, ?, ?)";

	public static void main(String[] args)  {
		boolean thorough = true;
		if (args.length > 0)
			thorough = Boolean.valueOf(args[0]);

		List<String> requiredArgs = Config.getRequiredArgsForWebdav();
		requiredArgs.addAll(Config.getRequiredArgsForDB("Current"));
		if (thorough)
			requiredArgs.addAll(IndexRecordListComparison.requiredArgs());
		else
			requiredArgs.addAll(Config.getRequiredArgsForDB("Voy"));

		try{        
			new IdentifyChangedRecords( Config.loadConfig( null, requiredArgs ),thorough);
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	public IdentifyChangedRecords(Config config, Boolean thorough) throws Exception {
		this.config = config;
		int retryLimit = 4;
		boolean succeeded = false;
		while (retryLimit > 0 && ! succeeded)
			try{

				if (thorough) {
					System.out.println("Launching thorough check for Voyager record changes.");
					thoroughIdentifiationOfChanges();
				} else {
//					System.out.println("Launching quick check for Voyager record changes.");
					quickIdentificationOfChanges();
				}
				succeeded = true;

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getClass().getName()+" identifying changed records."); System.exit(0);
				if (retryLimit-- > 0) {
					System.out.println("Will retry in 20 seconds.");
					Thread.sleep(20_000);
				} else {
					System.out.println("Retry limit reached. Failing.");
					throw e;
				}
			}
	}

	private void quickIdentificationOfChanges() throws Exception {
		updatedBibs.clear();
		try (   Connection current = config.getDatabaseConnection("Current");
				Statement stmtCurrent = current.createStatement() ) {

			if ( max_date == null )
				try ( ResultSet rs = stmtCurrent.executeQuery(maxBibTimestampQuery) ) {
					while (rs.next()) max_date = rs.getTimestamp(1);
				}

			Timestamp ts = max_date;
			ts.setTime(ts.getTime() - (10/*seconds*/
										* 1000/*millis per second*/));

			try ( Connection voyager = config.getDatabaseConnection("Voy") ){

				try ( PreparedStatement pstmt = voyager.prepareStatement(recentBibQuery) ){
					pstmt.setTimestamp(1, ts);
					pstmt.setTimestamp(2, ts);
					try ( ResultSet rs = pstmt.executeQuery() ){
						while (rs.next()) {
							Timestamp bibDate = rs.getTimestamp(3);
							if (bibDate == null) bibDate = rs.getTimestamp(2);
							String suppress_in_opac = rs.getString(4);
							int bib_id = rs.getInt(1);
							queueBib( current, bib_id, bibDate, 
									suppress_in_opac != null && suppress_in_opac.equals("N") );
							if (0 < bibDate.compareTo(max_date))
								max_date = bibDate;
						}
					}
				}

				int bibCount = updatedBibs.size();
				if ( bibCount > 0)
					System.out.println("Queued from poling bib data: "+bibCount);

				try ( PreparedStatement pstmt = voyager.prepareStatement( recentMfhdQuery )){
					pstmt.setTimestamp(1, ts);
					pstmt.setTimestamp(2, ts);
					try ( ResultSet rs = pstmt.executeQuery() ){
						while (rs.next()) {
							Timestamp mfhdDate = rs.getTimestamp(4);
							if (mfhdDate == null) mfhdDate = rs.getTimestamp(3);
							queueMfhd( current, rs.getInt(1), rs.getInt(2), mfhdDate, rs.getString(5).equals("N"));
						}
					}
				}

				int mfhdCount = updatedBibs.size() - bibCount;
				if ( mfhdCount > 0)
					System.out.println("Queued from poling holdings data: "+mfhdCount);

				try ( PreparedStatement pstmt = voyager.prepareStatement( recentItemQuery )){
					pstmt.setTimestamp(1, ts);
					pstmt.setTimestamp(2, ts);
					try ( ResultSet rs = pstmt.executeQuery() ){
						while (rs.next()) {
							Timestamp itemDate = rs.getTimestamp(4);
							if (itemDate == null) itemDate = rs.getTimestamp(3);
							queueItem( current, rs.getInt(1), rs.getInt(2), itemDate);
						}
					}
				}

				int itemCount = updatedBibs.size() - bibCount - mfhdCount;
				if ( itemCount > 0 )
					System.out.println("Queued from poling item data: "+itemCount);

				if ( ! updatedBibs.isEmpty() )
					System.out.println( (new Timestamp(System.currentTimeMillis())).toLocalDateTime().format(formatter)
							+" "+updatedBibs.toString() );
			}
		}
	}
	private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT,FormatStyle.MEDIUM);

	private void queueBib(Connection current, int bib_id, Timestamp update_date, Boolean isActive) throws SQLException {
		try ( PreparedStatement bibVoyQStmt = current.prepareStatement( bibVoyQuery ) ) {
			bibVoyQStmt.setInt(1, bib_id);
			try ( ResultSet rs = bibVoyQStmt.executeQuery() ) {
				while (rs.next()) {
					Timestamp old_date = rs.getTimestamp(1);
					Boolean previouslyActive = rs.getBoolean(2);
					if (update_date != null
							&& (old_date == null
							|| 0 > old_date.compareTo(update_date))) {
						// bib is already in current, but has been updated
						try ( PreparedStatement bibVoyUStmt = current.prepareStatement( bibVoyUpdate ) ){
							bibVoyUStmt.setTimestamp(1, update_date);
							bibVoyUStmt.setBoolean(2, isActive);
							bibVoyUStmt.setInt(3, bib_id);
							bibVoyUStmt.executeUpdate();
						}
						if (isActive) {
							updatedBibs.add(bib_id);
							if (previouslyActive) 
								addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.BIB_UPDATE, update_date);
							else
								addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ADD, update_date);
						} else {
							if (previouslyActive)
								queueBibDelete( current, bib_id );
							// else - ignore change to suppressed record
						}
					} // else bib is unchanged - do nothing
					return;
				}
			}
		}

		// bib is not yet in current db
		try ( PreparedStatement bibVoyIStmt = current.prepareStatement( bibVoyInsert ) ) {
			bibVoyIStmt.setInt(1, bib_id);
			bibVoyIStmt.setTimestamp(2, update_date);
			bibVoyIStmt.setBoolean(3, isActive);
			bibVoyIStmt.executeUpdate();
		}
		if ( isActive && ! updatedBibs.contains(bib_id) ) {
			updatedBibs.add(bib_id);
			addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ADD, update_date);
		}
	}

	private void queueMfhd(Connection current, int bib_id, int mfhd_id, Timestamp update_date, boolean active) throws SQLException {
		if ( ! isBibActive(current,bib_id))
			return;

		try ( PreparedStatement mfhdVoyQStmt = current.prepareStatement(mfhdVoyQuery) ) {
			mfhdVoyQStmt.setInt(1, mfhd_id);
			try ( ResultSet rs = mfhdVoyQStmt.executeQuery() ){
				while (rs.next()) {
					if (active) {
						Timestamp old_date = rs.getTimestamp(2);
						if (update_date != null
								&& (old_date == null
								|| 0 > old_date.compareTo(update_date))) {
							// mfhd is already in current, but has been updated
							int old_bib = rs.getInt(1);
							try ( PreparedStatement mfhdVoyUStmt = current.prepareStatement( mfhdVoyUpdate )){
								mfhdVoyUStmt.setTimestamp(1, update_date);
								mfhdVoyUStmt.setInt(2, bib_id);
								mfhdVoyUStmt.setInt(3, mfhd_id);
								mfhdVoyUStmt.executeUpdate();
							}
							if (! updatedBibs.contains(bib_id)) {
								addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE, update_date);
								updatedBibs.add(bib_id);
							}
	
							if (old_bib != bib_id
									&& ! updatedBibs.contains(old_bib)) {
								addBibToUpdateQueue(current, old_bib, DataChangeUpdateType.MFHD_UPDATE, update_date);
								updatedBibs.add(old_bib);
							}
						} // else mfhd is unchanged - do nothing
						return;
					}
					// holding has been suppressed
					try ( PreparedStatement mfhdVoyDelStmt = current.prepareStatement(mfhdVoyDelete) ) {
						mfhdVoyDelStmt.setInt(1, mfhd_id);
						mfhdVoyDelStmt.executeUpdate();
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.MFHD_DELETE,update_date);
						updatedBibs.add(bib_id);
					}
					return;
				}
			}
		}

		// mfhd is not yet in current db
		if (active)
			try ( PreparedStatement mfhdVoyIStmt = current.prepareStatement( mfhdVoyInsert ) ){
				mfhdVoyIStmt.setInt(1, bib_id);
				mfhdVoyIStmt.setInt(2, mfhd_id);
				mfhdVoyIStmt.setTimestamp(3, update_date);
				mfhdVoyIStmt.executeUpdate();
			}
		if (! updatedBibs.contains(bib_id)) {
			addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE,update_date);
			updatedBibs.add(bib_id);
		}
	}

	private void queueItem(Connection current, int mfhd_id, int item_id,
			Timestamp update_date) throws SQLException {

		try ( PreparedStatement itemVoyQStmt = current.prepareStatement( itemVoyQuery ) ){
			itemVoyQStmt.setInt(1, item_id);
			try ( ResultSet rs = itemVoyQStmt.executeQuery() ){
				while (rs.next()) {
					Timestamp old_date = rs.getTimestamp(2);
					if (update_date != null
							&& (old_date == null
							|| 0 > old_date.compareTo(update_date))) {
						// item is already in current, but has been updated
						int old_mfhd = rs.getInt(1);
						try ( PreparedStatement itemVoyUStmt = current.prepareStatement(itemVoyUpdate) ){
							itemVoyUStmt.setTimestamp(1, update_date);
							itemVoyUStmt.setInt(2, mfhd_id);
							itemVoyUStmt.setInt(3, item_id);
							itemVoyUStmt.executeUpdate();
						}

						int bib_id = getBibIdForMfhd(current, mfhd_id);
						if (bib_id > 0
								&& isBibActive(current,bib_id)
								&& ! updatedBibs.contains(bib_id)) {
							addBibToAvailQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE,update_date);
							updatedBibs.add(bib_id);
						}

						if (mfhd_id != old_mfhd) {
							int old_bib_id = getBibIdForMfhd(current, old_mfhd);
							if ( old_bib_id > 0
									&& old_bib_id != bib_id
									&& isBibActive(current,old_bib_id)
									&& ! updatedBibs.contains(old_bib_id)) {
								addBibToAvailQueue(current, old_bib_id, DataChangeUpdateType.ITEM_UPDATE,update_date);
								updatedBibs.add(old_bib_id);
							}
						}
					} // else item is unchanged - do nothing
					return;
				}
			}
		}

		// item is not yet in current db
		try ( PreparedStatement itemVoyIStmt = current.prepareStatement( itemVoyInsert ) ) {
			itemVoyIStmt.setInt(1, mfhd_id);
			itemVoyIStmt.setInt(2, item_id);
			itemVoyIStmt.setTimestamp(3, update_date);
			itemVoyIStmt.executeUpdate();
		}
		int bib_id = getBibIdForMfhd(current,mfhd_id);
		if (bib_id > 0
				&& isBibActive(current,bib_id)
				&& ! updatedBibs.contains(bib_id)) {
			addBibToAvailQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE,update_date);
			updatedBibs.add(bib_id);
		}
	}

	private static boolean isBibActive(Connection current, Integer bib_id) throws SQLException {
		boolean exists = false;
		try ( PreparedStatement bibVoyQStmt = current.prepareStatement( bibVoyQuery ) ){
			bibVoyQStmt.setInt(1, bib_id);
			try ( ResultSet rs = bibVoyQStmt.executeQuery() ){
				while (rs.next())
					exists = rs.getBoolean(2);
			}
		}
		return exists;
	}
	private static int getBibIdForMfhd(Connection current, Integer mfhd_id) throws SQLException {
		Integer bib_id = 0;
		try ( PreparedStatement mfhdVoyQStmt = current.prepareStatement(mfhdVoyQuery) ) {
			mfhdVoyQStmt.setInt(1, mfhd_id);
			try ( ResultSet rs = mfhdVoyQStmt.executeQuery() ){
				while (rs.next())
					bib_id = rs.getInt(1);
			}
		}
		return bib_id;
	}

	private void thoroughIdentifiationOfChanges() throws Exception {

		System.out.println("Comparing to contents of index at: " + config.getSolrUrl() );

		IndexRecordListComparison c = new IndexRecordListComparison(config);

		Set<Integer> bibsToAdd = c.bibsInVoyagerNotIndex();
		System.out.println("Bibs To Add to Solr: "+bibsToAdd.size());
		Set<Integer> bibsToDelete = c.bibsInIndexNotVoyager();
		System.out.println("Bibs To Delete from Solr: "+bibsToDelete.size());

		System.out.println("Bibs To Update:");

		Set<Integer> bibsToUpdate = c.bibsNewerInVoyagerThanIndex();
		System.out.println("\tbibsNewerInVoyagerThanIndex: "+bibsToUpdate.size());

		Map<Integer,Integer> tempMap = c.mfhdsInIndexNotVoyager();
		System.out.println("\tmfhdsInIndexNotVoyager: "+tempMap.size());
		Set<Integer> mfhdsToUpdate = new HashSet<>();
		mfhdsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsInVoyagerNotIndex();
		System.out.println("\tmfhdsInVoyagerNotIndex: "+tempMap.size());
		mfhdsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsNewerInVoyagerThanIndex();
		System.out.println("\tmfhdsNewerInVoyagerThanIndex: "+tempMap.size());
		mfhdsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		Map<Integer,ChangedBib> tempCBMap = c.mfhdsAttachedToDifferentBibs();
		System.out.println("\tmfhdsAttachedToDifferentBibs: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			mfhdsToUpdate.add(cb.original);
			mfhdsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();
/*
		tempMap = c.itemsNewerInVoyagerThanIndex();
		System.out.println("\titemsNewerInVoyagerThanIndex: "+tempMap.size());
		Set<Integer> itemsToUpdate = new HashSet<>();
		itemsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInIndexNotVoyager();
		System.out.println("\titemsInIndexNotVoyager: "+tempMap.size());
		itemsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInVoyagerNotIndex();
		System.out.println("\titemsInVoyagerNotIndex: "+tempMap.size());
		itemsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempCBMap = c.itemsAttachedToDifferentMfhds();
		System.out.println("\titemsAttachedToDifferentMfhds: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			itemsToUpdate.add(cb.original);
			itemsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();
*/
		c.queueBibs( bibsToAdd, DataChangeUpdateType.BIB_ADD );
		bibsToUpdate.removeAll(bibsToAdd);
		c.queueDeletes(bibsToDelete);
		bibsToUpdate.removeAll(bibsToDelete);
		c.queueBibs( bibsToUpdate, DataChangeUpdateType.BIB_UPDATE );
		mfhdsToUpdate.removeAll(bibsToDelete);
		mfhdsToUpdate.removeAll(bibsToAdd);
		mfhdsToUpdate.removeAll(bibsToUpdate);
		c.queueBibs( mfhdsToUpdate, DataChangeUpdateType.MFHD_UPDATE );
/*		itemsToUpdate.removeAll(bibsToDelete);
		itemsToUpdate.removeAll(bibsToAdd);
		itemsToUpdate.removeAll(bibsToUpdate);
		itemsToUpdate.removeAll(mfhdsToUpdate);
		c.queueBibs( itemsToUpdate, DataChangeUpdateType.ITEM_UPDATE );
		int totalBibsToUpdateCount = bibsToUpdate.size()+mfhdsToUpdate.size()+itemsToUpdate.size();

		System.out.println("Bibs To Update in Solr: "+totalBibsToUpdateCount);
*/
		c = null; // to allow GC
	
 	}

	public static enum DataChangeUpdateType {
		ADD("Added Record",5),
		BIB_ADD("Bibliographic Record Added",5),
		BIB_UPDATE("Bibliographic Record Update",5),
		MFHD_ADD("Holdings Record Added",5),
		MFHD_UPDATE("Holdings Record Change",5),
		MFHD_DELETE("Holdings Record Removed",5),
		ITEM_ADD("Item Record Added",3),
		ITEM_UPDATE("Item Record Change",3),
		ITEM_DELETE("Item Record Removed",3),
		TITLELINK("Title Link Update",7),
		
		AGE_IN_SOLR("Age of Record in Solr",6);

		private String string;
		private Integer priority;

		private DataChangeUpdateType(String name, Integer priority) {
			string = name;
			this.priority = priority;
		}

		public String toString() { return string; }
		public Integer getPriority () { return priority; }
	}
	
}
