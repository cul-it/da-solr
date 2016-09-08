package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForDB;
import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToUpdateQueue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class UpdateVoyagerInventory {

	final static String bibVoyInsert =
			"INSERT INTO "+CurrentDBTable.BIB_VOY+" (bib_id,record_date,active) VALUES (?,?,?)";
	final static String bibVoyQuery =
			"SELECT record_date, active FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?";
	final static String bibVoyUpdate =
			"UPDATE "+CurrentDBTable.BIB_VOY+" SET record_date = ?, active = ? WHERE bib_id = ?";
	final static String mfhdVoyUpdate =
			"UPDATE "+CurrentDBTable.MFHD_VOY+""+ " SET record_date = ?, bib_id = ? WHERE mfhd_id = ?";
	final static String mfhdVoyDelete =
			"DELETE FROM "+CurrentDBTable.MFHD_VOY+" WHERE mfhd_id = ?";
	final static String mfhdVoyInsert =
			"INSERT INTO "+CurrentDBTable.MFHD_VOY+" (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)";
	final static String itemVoyUpdate =
			"UPDATE "+CurrentDBTable.ITEM_VOY+" SET record_date = ?, mfhd_id = ? WHERE item_id = ?";
	final static String itemVoyInsert =
			"INSERT INTO "+CurrentDBTable.ITEM_VOY+" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)";
	final static String itemVoyDelete =
			"DELETE FROM "+CurrentDBTable.ITEM_VOY+" WHERE item_id = ?";
	final static String bibForMfhdQuery =
			"SELECT m.bib_id FROM "+CurrentDBTable.MFHD_VOY+" as m , "+CurrentDBTable.BIB_VOY+" as b "
			+"WHERE b.bib_id = m.bib_id AND m.mfhd_id = ? AND b.active = 1";

	public UpdateVoyagerInventory( SolrBuildConfig config ) throws ClassNotFoundException, SQLException {
	    try (   Connection voyager = config.getDatabaseConnection("Voy");
	    		Connection current = config.getDatabaseConnection("Current") ) {

    		updateBibVoyTable  ( voyager, current );
    		updateMfhdVoyTable ( voyager, current );
    		updateItemVoyTable ( voyager, current );

	    }

	}

	public static void main(String[] args) {

		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.addAll(getRequiredArgsForDB("Voy"));

		try{        
			new UpdateVoyagerInventory( SolrBuildConfig.loadConfig(args, requiredArgs) );
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void updateBibVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare bib lists between the two databases, and compile change lists.
		Map<Integer,DateAndStatus> newBibs = new HashMap<Integer,DateAndStatus>();
		Map<Integer,DateAndStatus> changedBibs = new HashMap<Integer,DateAndStatus>();
		Set<Integer> deletedBibs = new HashSet<Integer>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT bib_id, record_date, active FROM "+CurrentDBTable.BIB_VOY+" ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select BIB_ID, UPDATE_DATE, SUPPRESS_IN_OPAC"
						+ " from BIB_MASTER"
						+ " order by BIB_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager BIB_MASTER table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB "+CurrentDBTable.BIB_VOY+
						" table must not have zero records.");

			int c_id = 0, v_id = 0; 
			while ( ! c_rs.isAfterLast() && ! v_rs.isAfterLast() ) {
				c_id = c_rs.getInt(1);
				v_id = v_rs.getInt(1);
				Boolean v_active = (v_rs.getString(3).equals("N"))?true:false;
				Boolean c_active = c_rs.getBoolean(3);
				Timestamp v_date = v_rs.getTimestamp(2);
				Timestamp c_date = c_rs.getTimestamp(2);

				if ( c_id == v_id ) {

					if ( bibChanged( v_date, c_date, v_active, c_active) ) {

						changedBibs.put(v_id,new DateAndStatus(v_date,v_active));
					}
					v_rs.next();
					c_rs.next();

				} else if ( c_id < v_id ) {

					// deleted from Voyager
					deletedBibs.add(c_id);
					c_rs.next();

				} else { // c_id > v_id

					// added to Voyager
					newBibs.put(v_id,new DateAndStatus(v_date,v_active));
					v_rs.next();

				}
			}

			while ( ! v_rs.isAfterLast() ) {

				// added to Voyager
				newBibs.put(v_id,new DateAndStatus(v_rs.getTimestamp(2),(v_rs.getString(3).equals("N"))?true:false));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedBibs.add(c_id);
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newBibs.isEmpty() ) {
			try ( PreparedStatement bibVoyIStmt = current.prepareStatement( bibVoyInsert ) ) {
				for ( Entry<Integer,DateAndStatus> v_entry : newBibs.entrySet() ) {
					int bib_id = v_entry.getKey();
					bibVoyIStmt.setInt(1, bib_id);
					bibVoyIStmt.setTimestamp(2, v_entry.getValue().time);
					bibVoyIStmt.setBoolean(3, v_entry.getValue().active);
					bibVoyIStmt.addBatch();
					if (v_entry.getValue().active)
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ADD);
				}
				bibVoyIStmt.executeBatch();
			}
		}
		if ( ! deletedBibs.isEmpty() || ! changedBibs.isEmpty() ) {

			try (   PreparedStatement bibVoyUStmt = current.prepareStatement( bibVoyUpdate );
					PreparedStatement bibVoyQStmt = current.prepareStatement( bibVoyQuery ) ) {

				for ( Integer bib_id : deletedBibs ) {
					bibVoyQStmt.setInt(1, bib_id);
					Boolean c_active = null;
					Timestamp c_time = null;
					try ( ResultSet rs = bibVoyQStmt.executeQuery() ) {
						while (rs.next()) {
							c_time = rs.getTimestamp(1);
							c_active = rs.getBoolean(2);
						}
					}
					if ( ! c_active )
						continue;
					bibVoyUStmt.setTimestamp(1, c_time);
					bibVoyUStmt.setBoolean(2, false);
					bibVoyUStmt.setInt(3, bib_id);
					bibVoyUStmt.addBatch();
					addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.DELETE);
				}

				for ( Entry<Integer,DateAndStatus> v_entry : changedBibs.entrySet() ) {
					int bib_id = v_entry.getKey();
					bibVoyQStmt.setInt(1, bib_id);
					Boolean c_active = null;
					try ( ResultSet rs = bibVoyQStmt.executeQuery() ) {
						while (rs.next()) {
							c_active = rs.getBoolean(2);
						}
					}
					bibVoyUStmt.setTimestamp(1, v_entry.getValue().time);
					bibVoyUStmt.setBoolean(2, v_entry.getValue().active);
					bibVoyUStmt.setInt(3, bib_id);
					bibVoyUStmt.addBatch();
					if (v_entry.getValue().active) {
						if (c_active)
							addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.BIB_UPDATE);
						else
							addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ADD);
					} else if ( c_active ){
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.DELETE);
					}
				}

				bibVoyUStmt.executeBatch();
			}
		}
	}

	private static void updateMfhdVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare mfhd lists between the two databases, and compile change lists.
		Map<Integer,DateAndBib> newMfhds = new HashMap<Integer,DateAndBib>();
		Map<Integer,DateAndBib> changedMfhds = new HashMap<Integer,DateAndBib>();
		Map<Integer,Integer> deletedMfhds = new HashMap<Integer,Integer>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT mfhd_id, bib_id, record_date FROM "+CurrentDBTable.MFHD_VOY+" ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select MFHD_MASTER.MFHD_ID, BIB_MFHD.BIB_ID, UPDATE_DATE"
			    				+"  from BIB_MFHD, MFHD_MASTER"
			    				+" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
			    				+ "  and SUPPRESS_IN_OPAC = 'N'"
			    				+ " order by MFHD_MASTER.MFHD_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager MFHD_MASTER table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB "+CurrentDBTable.MFHD_VOY+
						" table must not have zero records.");

			int c_id = 0, v_id = 0; 
			while ( ! c_rs.isAfterLast() && ! v_rs.isAfterLast() ) {
				c_id = c_rs.getInt(1);
				v_id = v_rs.getInt(1);

				if ( c_id == v_id ) {

					Timestamp v_date = v_rs.getTimestamp(3);
					Timestamp c_date = c_rs.getTimestamp(3);
					if ( v_date != null && ! v_date.equals(c_date) ) {

						// mfhd changed
						int c_bib_id = c_rs.getInt(2);
						int v_bib_id = v_rs.getInt(2);
						if ( c_bib_id == v_bib_id )
							changedMfhds.put(v_id,new DateAndBib(v_date,v_bib_id));
						else
							changedMfhds.put(v_id,new DateAndBib(v_date,v_bib_id,c_bib_id));
					}
					v_rs.next();
					c_rs.next();

				} else if ( c_id < v_id ) {

					// deleted from Voyager
					deletedMfhds.put(c_id,c_rs.getInt(2));
					c_rs.next();

				} else { // c_id > v_id

					// added to Voyager
					newMfhds.put(v_id,new DateAndBib(v_rs.getTimestamp(3), v_rs.getInt(2)));
					v_rs.next();

				}
			}

			while ( ! v_rs.isAfterLast() ) {

				// added to Voyager
				newMfhds.put(v_id,new DateAndBib(v_rs.getTimestamp(3), v_rs.getInt(2)));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedMfhds.put(c_id,c_rs.getInt(2));
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newMfhds.isEmpty() ) {
			try ( PreparedStatement mfhdVoyIStmt = current.prepareStatement( mfhdVoyInsert ) ) {
				for ( Entry<Integer,DateAndBib> v_entry : newMfhds.entrySet() ) {
					int mfhd_id = v_entry.getKey();
					mfhdVoyIStmt.setInt(1, mfhd_id);
					mfhdVoyIStmt.setInt(2, v_entry.getValue().bib_id);
					mfhdVoyIStmt.setTimestamp(3, v_entry.getValue().time);
					mfhdVoyIStmt.addBatch();

					addBibToUpdateQueue(current, v_entry.getValue().bib_id, DataChangeUpdateType.MFHD_UPDATE);
				}
				mfhdVoyIStmt.executeBatch();
			}
		}

		if ( ! deletedMfhds.isEmpty() ) {

			try (   PreparedStatement mfhdVoyDStmt = current.prepareStatement( mfhdVoyDelete ) ) {
			
				for ( Entry<Integer,Integer> v_entry : deletedMfhds.entrySet() ) {
					mfhdVoyDStmt.setInt(1, v_entry.getKey());
					mfhdVoyDStmt.addBatch();
					addBibToUpdateQueue(current, v_entry.getValue(), DataChangeUpdateType.MFHD_UPDATE);
				}
				mfhdVoyDStmt.executeBatch();
			}
		}

		if ( ! changedMfhds.isEmpty() ) {

			try (   PreparedStatement mfhdVoyUStmt = current.prepareStatement( mfhdVoyUpdate )) {

				for ( Entry<Integer,DateAndBib> v_entry : changedMfhds.entrySet() ) {
					int mfhd_id = v_entry.getKey();
					int bib_id = v_entry.getValue().bib_id;
					mfhdVoyUStmt.setTimestamp(1, v_entry.getValue().time);
					mfhdVoyUStmt.setInt(2, bib_id);
					mfhdVoyUStmt.setInt(3, mfhd_id);
					mfhdVoyUStmt.addBatch();
					if (isBibActive(current,bib_id))
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE);
					Integer old_bib_id = v_entry.getValue().old_bib_id;
					if ( old_bib_id != null && isBibActive(current,old_bib_id))
						addBibToUpdateQueue(current, old_bib_id, DataChangeUpdateType.MFHD_UPDATE);
				}

				mfhdVoyUStmt.executeBatch();
			}
		}
	}

	private static void updateItemVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare mfhd lists between the two databases, and compile change lists.
		Map<Integer,DateAndHolding> newItems = new HashMap<Integer,DateAndHolding>();
		Map<Integer,DateAndHolding> changedItems = new HashMap<Integer,DateAndHolding>();
		Map<Integer,Integer> deletedItems = new HashMap<Integer,Integer>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT item_id, mfhd_id, record_date"
						+ " FROM "+CurrentDBTable.ITEM_VOY
						+" ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select ITEM.ITEM_ID, MFHD_ITEM.MFHD_ID, ITEM.MODIFY_DATE"
								+"  from MFHD_ITEM, ITEM"
								+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID"
								+" order by ITEM.ITEM_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager ITEM table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB "+CurrentDBTable.ITEM_VOY+
						" table must not have zero records.");

			int c_id = 0, v_id = 0; 
			while ( ! c_rs.isAfterLast() && ! v_rs.isAfterLast() ) {
				c_id = c_rs.getInt(1);
				v_id = v_rs.getInt(1);

				if ( c_id == v_id ) {

					Timestamp v_date = v_rs.getTimestamp(3);
					Timestamp c_date = c_rs.getTimestamp(3);
					if ( v_date != null && ! v_date.equals(c_date) ) {

						// item changed
						int c_mfhd_id = c_rs.getInt(2);
						int v_mfhd_id = v_rs.getInt(2);
						if ( c_mfhd_id == v_mfhd_id )
							changedItems.put(v_id,new DateAndHolding(v_date,v_mfhd_id));
						else
							changedItems.put(v_id,new DateAndHolding(v_date,v_mfhd_id,c_mfhd_id));
					}
					v_rs.next();
					c_rs.next();

				} else if ( c_id < v_id ) {

					// deleted from Voyager
					deletedItems.put(c_id,c_rs.getInt(2));
					c_rs.next();

				} else { // c_id > v_id

					// added to Voyager
					newItems.put(v_id,new DateAndHolding(v_rs.getTimestamp(3), v_rs.getInt(2)));
					v_rs.next();

				}
			}

			while ( ! v_rs.isAfterLast() ) {

				// added to Voyager
				newItems.put(v_id,new DateAndHolding(v_rs.getTimestamp(3), v_rs.getInt(2)));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedItems.put(c_id,c_rs.getInt(2));
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newItems.isEmpty() ) {
			try ( PreparedStatement itemVoyIStmt = current.prepareStatement( itemVoyInsert ) ) {
				for ( Entry<Integer,DateAndHolding> v_entry : newItems.entrySet() ) {
					int item_id = v_entry.getKey();
					int mfhd_id = v_entry.getValue().mfhd_id;
					itemVoyIStmt.setInt(1, item_id);
					itemVoyIStmt.setInt(2, mfhd_id);
					itemVoyIStmt.setTimestamp(3, v_entry.getValue().time);
					itemVoyIStmt.addBatch();

					Integer bib_id = bibForMfhd(current,mfhd_id);
					if (bib_id != null)
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE);
				}
				itemVoyIStmt.executeBatch();
			}
		}

		if ( ! deletedItems.isEmpty() ) {

			try (   PreparedStatement itemVoyDStmt = current.prepareStatement( itemVoyDelete ) ) {
			
				for ( Entry<Integer,Integer> v_entry : deletedItems.entrySet() ) {
					itemVoyDStmt.setInt(1, v_entry.getKey());
					itemVoyDStmt.addBatch();

					Integer bib_id = bibForMfhd(current,v_entry.getValue());
					if (bib_id != null)
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE);
				}
				itemVoyDStmt.executeBatch();
			}
		}

		if ( ! changedItems.isEmpty() ) {

			try (   PreparedStatement itemVoyUStmt = current.prepareStatement( itemVoyUpdate )) {

				for ( Entry<Integer,DateAndHolding> v_entry : changedItems.entrySet() ) {
					int item_id = v_entry.getKey();
					int mfhd_id = v_entry.getValue().mfhd_id;
					itemVoyUStmt.setTimestamp(1, v_entry.getValue().time);
					itemVoyUStmt.setInt(2, mfhd_id);
					itemVoyUStmt.setInt(3, item_id);
					itemVoyUStmt.addBatch();

					Integer bib_id = bibForMfhd(current, mfhd_id);
					if (bib_id != null)
						addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE);
					Integer old_bib_id = bibForMfhd(current, v_entry.getValue().old_mfhd_id);
					if ( old_bib_id != null )
						addBibToUpdateQueue(current, old_bib_id, DataChangeUpdateType.ITEM_UPDATE);
				}

				itemVoyUStmt.executeBatch();
			}
		}
	}

	private static boolean bibChanged(Timestamp v_date, Timestamp c_date, Boolean v_active, Boolean c_active) {
		if (v_date != null && ! v_date.equals(c_date))
			return true;
		if (! v_active.equals(c_active))
			return true;
		return false;
	}


	private static boolean isBibActive( Connection current, Integer bib_id ) throws SQLException {
		boolean bibActive = false;
		try ( PreparedStatement bibVoyQStmt = current.prepareStatement( bibVoyQuery ) ) {
			bibVoyQStmt.setInt(1,bib_id);
			try ( ResultSet rs = bibVoyQStmt.executeQuery() ) {
				while (rs.next())
					bibActive = rs.getBoolean(2);
			}
		}
		return bibActive;
	}
	private static Integer bibForMfhd( Connection current, Integer mfhd_id) throws SQLException {
		if (mfhd_id == null) return null;
		Integer bib_id = null;
		try ( PreparedStatement bibForMfhdStmt = current.prepareStatement( bibForMfhdQuery )) {
			bibForMfhdStmt.setInt(1, mfhd_id);
			try ( ResultSet rs = bibForMfhdStmt.executeQuery() ) {
				while (rs.next())
					bib_id = rs.getInt(1);
			}
		}
		return bib_id;
	}
	private static class DateAndStatus {
		Timestamp time;
		Boolean active;
		public DateAndStatus( Timestamp time, Boolean active ) {
			this.time = time;
			this.active = active;
		}
	}
	private static class DateAndBib {
		Timestamp time;
		Integer bib_id;
		Integer old_bib_id;
		public DateAndBib( Timestamp time, Integer bib_id) {
			this.time = time;
			this.bib_id = bib_id;
			this.old_bib_id = null;
		}
		public DateAndBib( Timestamp time, Integer bib_id, Integer old_bib_id ) {
			this.time = time;
			this.bib_id = bib_id;
			this.old_bib_id = old_bib_id;
		}
	}
	private static class DateAndHolding {
		Timestamp time;
		Integer mfhd_id;
		Integer old_mfhd_id;
		public DateAndHolding( Timestamp time, Integer mfhd_id) {
			this.time = time;
			this.mfhd_id = mfhd_id;
			this.old_mfhd_id = null;
		}
		public DateAndHolding( Timestamp time, Integer mfhd_id, Integer old_mfhd_id ) {
			this.time = time;
			this.mfhd_id = mfhd_id;
			this.old_mfhd_id = old_mfhd_id;
		}
	}

}
