package edu.cornell.library.integration.voyager;

import static edu.cornell.library.integration.utilities.Config.getRequiredArgsForDB;
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

import edu.cornell.library.integration.utilities.AddToQueue;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.voyager.IdentifyChangedRecords.DataChangeUpdateType;

public class UpdateVoyagerInventory {

	final static String bibVoyInsert =
			"INSERT INTO bibRecsVoyager (bib_id,record_date,active) VALUES (?,?,?)";
	final static String bibVoyQuery =
			"SELECT record_date, active FROM bibRecsVoyager WHERE bib_id = ?";
	final static String bibVoyUpdate =
			"UPDATE bibRecsVoyager SET record_date = ?, active = ? WHERE bib_id = ?";
	final static String mfhdVoyUpdate =
			"UPDATE mfhdRecsVoyager SET record_date = ?, bib_id = ? WHERE mfhd_id = ?";
	final static String mfhdVoyDelete =
			"DELETE FROM mfhdRecsVoyager WHERE mfhd_id = ?";
	final static String mfhdVoyInsert =
			"INSERT INTO mfhdRecsVoyager (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)";
	final static String itemVoyUpdate =
			"UPDATE itemRecsVoyager SET record_date = ?, mfhd_id = ? WHERE item_id = ?";
	final static String itemVoyInsert =
			"INSERT INTO itemRecsVoyager (mfhd_id, item_id, record_date) VALUES (?, ?, ?)";
	final static String itemVoyDelete =
			"DELETE FROM itemRecsVoyager WHERE item_id = ?";
	final static String bibForMfhdQuery =
			"SELECT m.bib_id FROM mfhdRecsVoyager as m , bibRecsVoyager as b "
			+"WHERE b.bib_id = m.bib_id AND m.mfhd_id = ? AND b.active = 1";

	public UpdateVoyagerInventory( Config config ) throws ClassNotFoundException, SQLException {
	    try (   Connection voyager = config.getDatabaseConnection("Voy");
	    		Connection current = config.getDatabaseConnection("Current") ) {

    		updateBibVoyTable  ( voyager, current );
    		updateMfhdVoyTable ( voyager, current );
    		updateItemVoyTable ( voyager, current );

	    }

	}

	public static void main(String[] args) {

		List<String> requiredArgs = new ArrayList<>();
		requiredArgs.addAll(getRequiredArgsForDB("Current"));
		requiredArgs.addAll(getRequiredArgsForDB("Voy"));

		try{        
			new UpdateVoyagerInventory( Config.loadConfig(args, requiredArgs) );
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void updateBibVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare bib lists between the two databases, and compile change lists.
		Map<Integer,DateAndStatus> newBibs = new HashMap<>();
		Map<Integer,DateAndStatus> changedBibs = new HashMap<>();
		Set<Integer> deletedBibs = new HashSet<>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT bib_id, record_date, active FROM bibRecsVoyager ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select BIB_ID, UPDATE_DATE, CREATE_DATE, SUPPRESS_IN_OPAC"
						+ " from BIB_MASTER"
						+ " order by BIB_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager BIB_MASTER table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB bibRecsVoyager table must not have zero records.");

			int c_id = 0, v_id = 0; 
			while ( ! c_rs.isAfterLast() && ! v_rs.isAfterLast() ) {
				c_id = c_rs.getInt(1);
				v_id = v_rs.getInt(1);
				Boolean v_active = (v_rs.getString(4).equals("N"))?true:false;
				Boolean c_active = c_rs.getBoolean(3);
				Timestamp v_date = v_rs.getTimestamp(2);
				if (v_date == null)
					v_date = v_rs.getTimestamp(3);
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
				Timestamp v_date = v_rs.getTimestamp(2);
				if (v_date == null)
					v_date = v_rs.getTimestamp(3);

				newBibs.put(v_rs.getInt(1),new DateAndStatus(v_date,(v_rs.getString(4).equals("N"))?true:false));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedBibs.add(c_rs.getInt(1));
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newBibs.isEmpty() ) {
			System.out.println(newBibs.size()+" new bibs.");
			int queuedCount = 0;
			try ( PreparedStatement bibVoyIStmt = current.prepareStatement( bibVoyInsert ) ) {

				int i = 0;
				for ( Entry<Integer,DateAndStatus> v_entry : newBibs.entrySet() ) {
					int bib_id = v_entry.getKey();
					bibVoyIStmt.setInt(1, bib_id);
					bibVoyIStmt.setTimestamp(2, v_entry.getValue().time);
					bibVoyIStmt.setBoolean(3, v_entry.getValue().active);
					bibVoyIStmt.addBatch();
					if (v_entry.getValue().active)
						if ( queue(current, bib_id, DataChangeUpdateType.ADD, v_entry.getValue().time) )
							queuedCount++;
					if ( ++i % 10_000 == 0)
						bibVoyIStmt.executeBatch();
				}
				bibVoyIStmt.executeBatch();
			}
			System.out.println("\t"+queuedCount+" bibs queued.");
		}
		if ( ! deletedBibs.isEmpty() || ! changedBibs.isEmpty() ) {
			System.out.println(deletedBibs.size()+" deleted bibs.");

			try (   PreparedStatement bibVoyUStmt = current.prepareStatement( bibVoyUpdate );
					PreparedStatement bibVoyQStmt = current.prepareStatement( bibVoyQuery );
					PreparedStatement bibDeleteQueueStmt = AddToQueue.deleteQueueStmt(current)) {

				int i = 0;
				int queuedCount = 0;
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
					if ( c_active != null && ! c_active )
						continue;
					bibVoyUStmt.setTimestamp(1, c_time);
					bibVoyUStmt.setBoolean(2, false);
					bibVoyUStmt.setInt(3, bib_id);
					bibVoyUStmt.addBatch();
					AddToQueue.add2DeleteQueueBatch(bibDeleteQueueStmt, bib_id);
					queuedCount++;
					if ( ++i % 10_000 == 0) {
						bibDeleteQueueStmt.executeBatch();
						bibVoyUStmt.executeBatch();
					}
				}
				System.out.println("\t"+queuedCount+" bibs deletes queued.");

				int queuedCountUpd = 0;
				int queuedCountAdd = 0;
				int queuedCountDel = 0;
				System.out.println(changedBibs.size()+" changed bibs.");
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
						if ( c_active != null && c_active ) {
							if ( queue(current, bib_id, DataChangeUpdateType.BIB_UPDATE, v_entry.getValue().time) )
								queuedCountUpd++;
						} else
							if ( queue(current, bib_id, DataChangeUpdateType.ADD, v_entry.getValue().time) )
								queuedCountAdd++;
					} else if ( c_active != null && c_active ){
						AddToQueue.add2DeleteQueueBatch(bibDeleteQueueStmt, bib_id);
						queuedCountDel++;
					}
					if ( ++i % 10_000 == 0) {
						bibVoyUStmt.executeBatch();
						bibDeleteQueueStmt.executeBatch();
					}
				}
				if ( queuedCountUpd > 0 )
					System.out.println("\t"+queuedCountUpd+" bib updates queued.");
				if ( queuedCountAdd > 0 )
					System.out.println("\t"+queuedCountAdd+" bib adds queued.");
				if ( queuedCountDel > 0 )
					System.out.println("\t"+queuedCountDel+" bib deletes queued.");

				bibVoyUStmt.executeBatch();
				bibDeleteQueueStmt.executeBatch();
			}
		}
	}

	private static void updateMfhdVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare mfhd lists between the two databases, and compile change lists.
		Map<Integer,DateAndBib> newMfhds = new HashMap<>();
		Map<Integer,DateAndBib> changedMfhds = new HashMap<>();
		Map<Integer,Integer> deletedMfhds = new HashMap<>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT mfhd_id, m.bib_id, m.record_date, b.active"
						+ " FROM mfhdRecsVoyager AS m"
						+ " LEFT JOIN bibRecsVoyager AS b ON b.bib_id = m.bib_id"
						+ " ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select MFHD_MASTER.MFHD_ID, BIB_MFHD.BIB_ID, UPDATE_DATE, CREATE_DATE"
			    				+"  from BIB_MFHD, MFHD_MASTER"
			    				+" where BIB_MFHD.MFHD_ID = MFHD_MASTER.MFHD_ID"
			    				+ "  and SUPPRESS_IN_OPAC = 'N'"
			    				+ " order by MFHD_MASTER.MFHD_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager MFHD_MASTER table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB mfhdRecsVoyager table must not have zero records.");

			int c_id = 0, v_id = 0; 
			while ( ! c_rs.isAfterLast() && ! v_rs.isAfterLast() ) {
				c_id = c_rs.getInt(1);
				v_id = v_rs.getInt(1);

				if ( c_id == v_id ) {

					Timestamp v_date = v_rs.getTimestamp(3);
					if (v_date == null)
						v_date = v_rs.getTimestamp(4);
					Timestamp c_date = c_rs.getTimestamp(3);
					int c_bib_id = c_rs.getInt(2);
					if ( ! c_rs.getBoolean(4) ) {

						// if bib is now suppressed treat this as though the mfhd is gone
						// regardless of whether the mfhd has been modified
						deletedMfhds.put(c_id, c_bib_id);

					} else if ( v_date != null && ! v_date.equals(c_date) ) {

						// mfhd changed
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
					Timestamp v_date = v_rs.getTimestamp(3);
					if (v_date == null)
						v_date = v_rs.getTimestamp(4);
					newMfhds.put(v_id,new DateAndBib(v_date, v_rs.getInt(2)));
					v_rs.next();

				}
			}

			while ( ! v_rs.isAfterLast() ) {

				// added to Voyager
				Timestamp v_date = v_rs.getTimestamp(3);
				if (v_date == null)
					v_date = v_rs.getTimestamp(4);
				newMfhds.put(v_rs.getInt(1),new DateAndBib(v_date, v_rs.getInt(2)));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedMfhds.put(c_rs.getInt(1),c_rs.getInt(2));
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newMfhds.isEmpty() ) {
			System.out.println(newMfhds.size()+" new mfhds.");
			try ( PreparedStatement mfhdVoyIStmt = current.prepareStatement( mfhdVoyInsert ) ) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,DateAndBib> v_entry : newMfhds.entrySet() ) {
					int mfhd_id = v_entry.getKey();
					int bib_id = v_entry.getValue().bib_id;

					if ( ! isBibActive(current,bib_id))
						continue;

					mfhdVoyIStmt.setInt(1, bib_id);
					mfhdVoyIStmt.setInt(2, mfhd_id);
					mfhdVoyIStmt.setTimestamp(3, v_entry.getValue().time);
					mfhdVoyIStmt.addBatch();
					if ( queue(current, bib_id, DataChangeUpdateType.MFHD_ADD, v_entry.getValue().time) )
						queuedCount++;
					if ( ++i % 10_000 == 0)
						mfhdVoyIStmt.executeBatch();
				}
				mfhdVoyIStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");
			}
		}

		if ( ! deletedMfhds.isEmpty() ) {
			System.out.println(deletedMfhds.size()+" deleted mfhds.");

			try (   PreparedStatement mfhdVoyDStmt = current.prepareStatement( mfhdVoyDelete ) ) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,Integer> v_entry : deletedMfhds.entrySet() ) {
					mfhdVoyDStmt.setInt(1, v_entry.getKey());
					mfhdVoyDStmt.addBatch();
					int v_bib_id = v_entry.getValue();
					if ( isBibActive( current , v_bib_id ))
						if ( queue(current, v_bib_id, DataChangeUpdateType.MFHD_DELETE, new Timestamp(System.currentTimeMillis())) )
							queuedCount++;
					if ( ++i % 10_000 == 0)
						mfhdVoyDStmt.executeBatch();
				}
				mfhdVoyDStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");
			}
		}

		if ( ! changedMfhds.isEmpty() ) {
			System.out.println(changedMfhds.size()+" changed mfhds.");

			try (   PreparedStatement mfhdVoyUStmt = current.prepareStatement( mfhdVoyUpdate );
					PreparedStatement mfhdVoyDStmt = current.prepareStatement( mfhdVoyDelete )) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,DateAndBib> v_entry : changedMfhds.entrySet() ) {
					int mfhd_id = v_entry.getKey();
					int bib_id = v_entry.getValue().bib_id;
					Integer old_bib_id = v_entry.getValue().old_bib_id;
					if (isBibActive(current,bib_id)) {
						if (old_bib_id != null) {
							if ( queue(current, bib_id, DataChangeUpdateType.MFHD_ADD, v_entry.getValue().time) )
								queuedCount++;
						} else
							if ( queue(current, bib_id, DataChangeUpdateType.MFHD_UPDATE, v_entry.getValue().time) )
								queuedCount++;
						mfhdVoyUStmt.setTimestamp(1, v_entry.getValue().time);
						mfhdVoyUStmt.setInt(2, bib_id);
						mfhdVoyUStmt.setInt(3, mfhd_id);
						mfhdVoyUStmt.addBatch();
						if ( ++i % 10_000 == 0)
							mfhdVoyUStmt.executeBatch();
					} else {
						mfhdVoyDStmt.setInt(1, mfhd_id);
						mfhdVoyDStmt.executeUpdate();
					}
					if ( old_bib_id != null && isBibActive(current,old_bib_id))
						if ( queue(current, old_bib_id, DataChangeUpdateType.MFHD_DELETE,  new Timestamp(System.currentTimeMillis())) )
							queuedCount++;
				}
				mfhdVoyUStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");

			}
		}
	}

	private static void updateItemVoyTable(Connection voyager, Connection current) throws SQLException {

		// Step 1: Compare mfhd lists between the two databases, and compile change lists.
		Map<Integer,DateAndHolding> newItems = new HashMap<>();
		Map<Integer,DateAndHolding> changedItems = new HashMap<>();
		Map<Integer,Integer> deletedItems = new HashMap<>();

		try (   Statement c_stmt = current.createStatement();
				Statement v_stmt = voyager.createStatement();
				ResultSet c_rs = c_stmt.executeQuery(
						"SELECT item_id, mfhd_id, record_date FROM itemRecsVoyager ORDER BY 1");
				ResultSet v_rs = v_stmt.executeQuery(
						"select ITEM.ITEM_ID, MFHD_ITEM.MFHD_ID, ITEM.MODIFY_DATE"
								+"  from MFHD_ITEM, ITEM"
								+" where MFHD_ITEM.ITEM_ID = ITEM.ITEM_ID"
								+" order by ITEM.ITEM_ID")  ){

			if ( ! c_rs.next() )
				throw new SQLException("Error: Voyager ITEM table must not have zero records.");
			if ( ! v_rs.next() )
				throw new SQLException("Error: InventoryDB itemRecsVoyager table must not have zero records.");

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
				newItems.put(v_rs.getInt(1),new DateAndHolding(v_rs.getTimestamp(3), v_rs.getInt(2)));
				v_rs.next();

			}

			while ( ! c_rs.isAfterLast() ) {

				// deleted from Voyager
				deletedItems.put(c_rs.getInt(1),c_rs.getInt(2));
				c_rs.next();

			}
		}

		// Step 2: Update inventory database and indexing queue.
		if ( ! newItems.isEmpty() ) {
			System.out.println(newItems.size()+" new items.");
			try ( PreparedStatement itemVoyIStmt = current.prepareStatement( itemVoyInsert ) ) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,DateAndHolding> v_entry : newItems.entrySet() ) {
					int item_id = v_entry.getKey();
					int mfhd_id = v_entry.getValue().mfhd_id;
					Integer bib_id = bibForMfhd(current,mfhd_id);

					if (bib_id == null)
						continue;

					itemVoyIStmt.setInt(1, mfhd_id);
					itemVoyIStmt.setInt(2, item_id);
					itemVoyIStmt.setTimestamp(3, v_entry.getValue().time);
					itemVoyIStmt.addBatch();
					if ( queue(current, bib_id, DataChangeUpdateType.ITEM_ADD, v_entry.getValue().time) )
						queuedCount++;
					if ( ++i % 10_000 == 0)
						itemVoyIStmt.executeBatch();
				}
				itemVoyIStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");
			}
		}

		if ( ! deletedItems.isEmpty() ) {
			System.out.println(deletedItems.size()+" deleted items.");

			try (   PreparedStatement itemVoyDStmt = current.prepareStatement( itemVoyDelete ) ) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,Integer> v_entry : deletedItems.entrySet() ) {
					itemVoyDStmt.setInt(1, v_entry.getKey());
					itemVoyDStmt.addBatch();

					Integer bib_id = bibForMfhd(current,v_entry.getValue());
					if (bib_id != null)
						if ( queue(current, bib_id, DataChangeUpdateType.ITEM_DELETE, new Timestamp(System.currentTimeMillis())) )
							queuedCount++;
					if ( ++i % 10_000 == 0)
						itemVoyDStmt.executeBatch();
				}
				itemVoyDStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");
			}
		}

		if ( ! changedItems.isEmpty() ) {
			System.out.println(changedItems.size()+" changed items.");

			try (   PreparedStatement itemVoyUStmt = current.prepareStatement( itemVoyUpdate );
					PreparedStatement itemVoyDStmt = current.prepareStatement( itemVoyDelete )) {

				int i = 0;
				int queuedCount = 0;
				for ( Entry<Integer,DateAndHolding> v_entry : changedItems.entrySet() ) {
					int item_id = v_entry.getKey();
					int mfhd_id = v_entry.getValue().mfhd_id;
					Integer bib_id = bibForMfhd(current, mfhd_id);
					Integer old_bib_id = bibForMfhd(current, v_entry.getValue().old_mfhd_id);

					if (bib_id != null) {
						if (old_bib_id != null) {
							if ( queue(current, bib_id, DataChangeUpdateType.ITEM_ADD, v_entry.getValue().time) )
								queuedCount++;
						} else
							if ( queue(current, bib_id, DataChangeUpdateType.ITEM_UPDATE, v_entry.getValue().time) )
								queuedCount++;
						itemVoyUStmt.setTimestamp(1, v_entry.getValue().time);
						itemVoyUStmt.setInt(2, mfhd_id);
						itemVoyUStmt.setInt(3, item_id);
						itemVoyUStmt.addBatch();
						if ( ++i % 10_000 == 0)
							itemVoyUStmt.executeBatch();
					} else {
						itemVoyDStmt.setInt(1, item_id);
						itemVoyDStmt.executeUpdate();
					}
					if ( old_bib_id != null )
						if ( queue(current, old_bib_id, DataChangeUpdateType.ITEM_DELETE, new Timestamp(System.currentTimeMillis())) )
							queuedCount++;
				}
				itemVoyUStmt.executeBatch();
				System.out.println("\t"+queuedCount+" bibs queued.");
			}
		}
	}

	final static Set<Integer> queuedBibs = new HashSet<>();
	private static boolean queue( Connection current, Integer bib_id, DataChangeUpdateType type, Timestamp recordDate) throws SQLException {
		if (queuedBibs.contains(bib_id))
			return false;
		addBibToUpdateQueue(current, bib_id, type, recordDate);
		queuedBibs.add(bib_id);
		return true;
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
