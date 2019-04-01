package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import edu.cornell.library.integration.indexer.queues.AddToQueue;
import edu.cornell.library.integration.voyager.IdentifyChangedRecords.DataChangeUpdateType;


/**
 * This class is intended to compare the current voyager BIB and MFHD
 * records with the records in a Solr index. It will create a list of 
 * records that have been removed from Voyager or that are missing 
 * from the Solr Index.
 * 
 * To use this class, make a new IndexRecordListComparison and then
 * call compare().  After the call to compare() the properties
 * bibsInIndexNotVoyager, bibsInVoyagerNotIndex, mfhdsInIndexNotVoyager
 * and mfhdsInVoyagerNotIndex will then be set.
 * 
 */
public class IndexRecordListComparison {



	private Connection conn = null;
	private Statement stmt = null;
	private Map<String,PreparedStatement> pstmts = new HashMap<>();

	
	public static List<String> requiredArgs() {
		List<String> l = new ArrayList<>();
		l.addAll(Config.getRequiredArgsForDB("Current"));
		l.add("solrUrl");
		return l;
	}
	
	public IndexRecordListComparison(Config config) throws ClassNotFoundException, SQLException {

		conn = config.getDatabaseConnection("Current");
		stmt = conn.createStatement();

	}

	public Map<Integer,ChangedBib> mfhdsAttachedToDifferentBibs() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id, s.bib_id "
				+ "FROM mfhdRecsVoyager as v, mfhdRecsSolr as s "
				+"WHERE v.mfhd_id = s.mfhd_id AND v.bib_id != s.bib_id") ) {
			while (rs.next())
				m.put(rs.getInt(1), new ChangedBib(rs.getInt(3),rs.getInt(2)));
		}
		return m;
	}
	
	public Map<Integer,ChangedBib> itemsAttachedToDifferentMfhds() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id, s.mfhd_id "
				+ "FROM itemRecsVoyager as v, itemRecsSolr as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND v.mfhd_id != s.mfhd_id") ){
			while (rs.next())
				m.put(rs.getInt(1),
						new ChangedBib(getBibForMfhd("mfhdRecsSolr",rs.getInt(3)),
								getBibForMfhd("mfhdRecsVoyager",rs.getInt(2))));
		}
		return m;
	}

	
	public Map<Integer,Integer> itemsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<>();
		// new items
		try ( ResultSet rs = stmt.executeQuery(
				"select v.item_id, v.mfhd_id"
				+ " from itemRecsVoyager as v "
				+ "left join itemRecsSolr as s on s.item_id = v.item_id "
				+ "where s.mfhd_id is null") ) {
			while (rs.next())
				m.put(rs.getInt(1),getBibForMfhd("mfhdRecsVoyager",rs.getInt(2)));
		}
		return m;
	}

	
	public Map<Integer,Integer> itemsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new HashMap<>();
		// deleted items
		try ( ResultSet rs = stmt.executeQuery(
				"select s.item_id, s.mfhd_id "
				+ "from itemRecsSolr as s "
				+ "left join itemRecsVoyager as v on s.item_id = v.item_id "
				+ "where v.mfhd_id is null") ){
			while (rs.next()) m.put(rs.getInt(1),getBibForMfhd("mfhdRecsVoyager",rs.getInt(2)));
		}
		return m;
	}
	
	public Map<Integer,Integer> itemsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id "
				+ "FROM itemRecsVoyager as v, itemRecsSolr as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND (v.record_date > date_add(s.record_date,interval 15 second) "
				+ "     OR ( v.record_date is not null AND s.record_date is null))")){
			while (rs.next())
				m.put(rs.getInt(1),getBibForMfhd("mfhdRecsVoyager",rs.getInt(2)));
		}
		return m;
	}

	
	public Map<Integer,Integer> mfhdsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new TreeMap<>();
		try ( ResultSet rs = stmt.executeQuery(
				"select v.mfhd_id, v.bib_id"
				+ " from mfhdRecsVoyager as v "
				+ "left join mfhdRecsSolr as s on s.mfhd_id = v.mfhd_id "
				+ "where s.bib_id is null") ) {
			while (rs.next())
				m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Map<Integer,Integer> mfhdsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new TreeMap<>();
		// deleted mfhds
		try ( ResultSet rs = stmt.executeQuery(
				"select s.mfhd_id, s.bib_id"
				+ " from mfhdRecsSolr as s "
				+ "left join mfhdRecsVoyager as v on s.mfhd_id = v.mfhd_id "
				+ "where v.bib_id is null") ) {
			while (rs.next()) m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Map<Integer,Integer> mfhdsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<>();
		// updated holdings
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id "
				+ "FROM mfhdRecsVoyager as v, mfhdRecsSolr as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND (v.record_date > date_add(s.record_date,interval 15 second) "
				+ "     OR ( v.record_date is not null AND s.record_date is null))") ) {
			while (rs.next())
				m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Set<Integer> bibsInVoyagerNotIndex() throws SQLException {
		Set<Integer> l = new TreeSet<>();
		try ( ResultSet rs = stmt.executeQuery(
				"select v.bib_id from bibRecsVoyager as v "
				+ "left join bibRecsSolr as s on s.bib_id = v.bib_id "
				+ "where s.bib_id is null AND v.active = 1") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}
	
	public Set<Integer> bibsInIndexNotVoyager() throws SQLException {
		
		Set<Integer> l = new TreeSet<>();
		try ( ResultSet rs = stmt.executeQuery(
				"select s.bib_id from bibRecsSolr as s "
				+ "left join bibRecsVoyager as v on s.bib_id = v.bib_id "
				+ "where s.active = 1 AND ( v.bib_id is null OR v.active = 0 )") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}

	public Set<Integer> bibsNewerInVoyagerThanIndex() throws SQLException {
		Set<Integer> l = new TreeSet<>();
		try (   Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(
				"SELECT v.bib_id"
				+ " FROM bibRecsVoyager as v, bibRecsSolr as s "
				+ "WHERE v.bib_id = s.bib_id"
				+ "  AND v.record_date > date_add(s.record_date,interval 15 second)"
				+ "  AND v.active = 1") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}

	@SuppressWarnings("resource") // for preparedstatement
	private int getBibForMfhd( String table, int mfhdId ) throws SQLException {
		String statementKey = "mfhd2bib_"+table;
		if ( ! pstmts.containsKey(statementKey))
			pstmts.put(statementKey, conn.prepareStatement(
					"SELECT bib_id FROM "+table+" WHERE mfhd_id = ?"));
		int bibid = 0;
		PreparedStatement pstmt = pstmts.get(statementKey);
		pstmt.setInt(1, mfhdId);
		try ( ResultSet rs = pstmt.executeQuery() ) {
			while (rs.next())
				bibid = rs.getInt(1);
		}
		return bibid;
	}

	
	public void queueBibs(Set<Integer> bibsToAdd, DataChangeUpdateType type) throws Exception {
		if ( bibsToAdd == null || bibsToAdd.isEmpty() ) return;

		if ( type.toString().startsWith("Bibliographic") || type.toString().startsWith("Holdings") ) {
			System.out.printf("Queuing %d bibs to generationQueue due to %s.\n", bibsToAdd.size(), type.toString());
			try( PreparedStatement pstmt = AddToQueue.generationQueueStmt(conn) ){

				int i = 0;
				for (Integer bibId : bibsToAdd ) {
					AddToQueue.add2QueueBatch(pstmt, bibId, new Timestamp(System.currentTimeMillis()), type);
					if ( ++i % 1_000 == 0 )
						pstmt.executeBatch();
				}
				pstmt.executeBatch();
			}
		}

		if ( type.toString().startsWith("Item") || type.toString().startsWith("Holdings") ) {
			System.out.printf("Queuing %d bibs to availabilityQueue due to %s.\n", bibsToAdd.size(), type.toString());
			try( PreparedStatement pstmt = AddToQueue.availabilityQueueStmt(conn) ){
				for (Integer bibId : bibsToAdd )
					AddToQueue.add2QueueBatch(pstmt, bibId, new Timestamp(System.currentTimeMillis()), type);
				pstmt.executeBatch();
			}
		}
	}

	public void queueDeletes( Set<Integer> bibsToDelete ) throws SQLException {
		if (bibsToDelete == null || bibsToDelete.isEmpty())
			return;
		try ( PreparedStatement pstmt = AddToQueue.deleteQueueStmt(conn) ) {
			for (Integer bib : bibsToDelete) {
				if (bib == null || bib.equals(0)) continue;
				AddToQueue.add2DeleteQueueBatch(pstmt, bib);
			}
			pstmt.executeBatch();
		}
	}

	public class ChangedBib {
		public int original;
		public int changed;
		private ChangedBib( int original, int changed ) {
			this.original = original;
			this.changed = changed;
		}
	}

}
