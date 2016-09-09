package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.IndexingUtilities.IndexQueuePriority;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;


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
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();

	
	public static List<String> requiredArgs() {
		List<String> l = new ArrayList<String>();
		l.addAll(SolrBuildConfig.getRequiredArgsForDB("Current"));
		l.add("solrUrl");
		return l;
	}
	
	public IndexRecordListComparison(SolrBuildConfig config) throws ClassNotFoundException, SQLException {

		conn = config.getDatabaseConnection("Current");
		stmt = conn.createStatement();

	}

	public Map<Integer,ChangedBib> mfhdsAttachedToDifferentBibs() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<Integer,ChangedBib>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id, s.bib_id "
				+ "FROM "+CurrentDBTable.MFHD_VOY+" as v, "
						+CurrentDBTable.MFHD_SOLR+" as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND v.bib_id != s.bib_id") ) {
			while (rs.next())
				m.put(rs.getInt(1), new ChangedBib(rs.getInt(3),rs.getInt(2)));
		}
		return m;
	}
	
	public Map<Integer,ChangedBib> itemsAttachedToDifferentMfhds() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<Integer,ChangedBib>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id, s.mfhd_id "
				+ "FROM "+CurrentDBTable.ITEM_VOY+" as v, "
						+CurrentDBTable.ITEM_SOLR+ " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND v.mfhd_id != s.mfhd_id") ){
			while (rs.next())
				m.put(rs.getInt(1),
						new ChangedBib(getBibForMfhd(CurrentDBTable.MFHD_SOLR,rs.getInt(3)),
								getBibForMfhd(CurrentDBTable.MFHD_VOY,rs.getInt(2))));
		}
		return m;
	}

	
	public Map<Integer,Integer> itemsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// new items
		try ( ResultSet rs = stmt.executeQuery(
				"select v.item_id, v.mfhd_id"
				+ " from "+CurrentDBTable.ITEM_VOY+" as v "
				+ "left join "+CurrentDBTable.ITEM_SOLR+" as s"
						+ " on s.item_id = v.item_id "
				+ "where s.mfhd_id is null") ) {
			while (rs.next())
				m.put(rs.getInt(1),getBibForMfhd(CurrentDBTable.MFHD_VOY,rs.getInt(2)));
		}
		return m;
	}

	
	public Map<Integer,Integer> itemsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// deleted items
		try ( ResultSet rs = stmt.executeQuery(
				"select s.item_id, s.mfhd_id "
				+ "from "+CurrentDBTable.ITEM_SOLR+" as s "
				+ "left join "+CurrentDBTable.ITEM_VOY+" as v"
						+ " on s.item_id = v.item_id "
				+ "where v.mfhd_id is null") ){
			while (rs.next()) m.put(rs.getInt(1),getBibForMfhd(CurrentDBTable.MFHD_VOY,rs.getInt(2)));
		}
		return m;
	}
	
	public Map<Integer,Integer> itemsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id "
				+ "FROM "+CurrentDBTable.ITEM_VOY+" as v,"
						+CurrentDBTable.ITEM_SOLR+ " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND (v.record_date > date_add(s.record_date,interval 15 second) "
				+ "     OR ( v.record_date is not null AND s.record_date is null))")){
			while (rs.next())
				m.put(rs.getInt(1),getBibForMfhd(CurrentDBTable.MFHD_VOY,rs.getInt(2)));
		}
		return m;
	}

	
	public Map<Integer,Integer> mfhdsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		try ( ResultSet rs = stmt.executeQuery(
				"select v.mfhd_id, v.bib_id"
				+ " from "+CurrentDBTable.MFHD_VOY+" as v "
				+ "left join "+CurrentDBTable.MFHD_SOLR+" as s"
						+ " on s.mfhd_id = v.mfhd_id "
				+ "where s.bib_id is null") ) {
			while (rs.next())
				m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Map<Integer,Integer> mfhdsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// deleted mfhds
		try ( ResultSet rs = stmt.executeQuery(
				"select s.mfhd_id, s.bib_id"
				+ " from "+CurrentDBTable.MFHD_SOLR+" as s "
				+ "left join "+CurrentDBTable.MFHD_VOY+" as v"
						+ " on s.mfhd_id = v.mfhd_id "
				+ "where v.bib_id is null") ) {
			while (rs.next()) m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Map<Integer,Integer> mfhdsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// updated holdings
		try ( ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id "
				+ "FROM "+CurrentDBTable.MFHD_VOY+" as v,"
						+CurrentDBTable.MFHD_SOLR+" as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND (v.record_date > date_add(s.record_date,interval 15 second) "
				+ "     OR ( v.record_date is not null AND s.record_date is null))") ) {
			while (rs.next())
				m.put(rs.getInt(1),rs.getInt(2));
		}
		return m;
	}
	
	public Set<Integer> bibsInVoyagerNotIndex() throws SQLException {
		Set<Integer> l = new HashSet<Integer>();
		try ( ResultSet rs = stmt.executeQuery(
				"select v.bib_id from "+CurrentDBTable.BIB_VOY+" as v "
				+ "left join "+CurrentDBTable.BIB_SOLR+" as s"
						+ " on s.bib_id = v.bib_id "
				+ "where s.bib_id is null AND v.active = 1") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}
	
	public Set<Integer> bibsInIndexNotVoyager() throws SQLException {
		
		Set<Integer> l = new HashSet<Integer>();
		try ( ResultSet rs = stmt.executeQuery(
				"select s.bib_id from "+CurrentDBTable.BIB_SOLR+" as s "
				+ "left join "+CurrentDBTable.BIB_VOY+" as v"
						+ " on s.bib_id = v.bib_id "
				+ "where s.active = 1 AND ( v.bib_id is null OR v.active = 0 )") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}

	public Set<Integer> bibsNewerInVoyagerThanIndex() throws SQLException {
		Set<Integer> l = new HashSet<Integer>();
		try (   Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(
				"SELECT v.bib_id"
				+ " FROM "+CurrentDBTable.BIB_VOY+" as v, "
						+CurrentDBTable.BIB_SOLR+" as s "
				+ "WHERE v.bib_id = s.bib_id"
				+ "  AND v.record_date > date_add(s.record_date,interval 15 second)"
				+ "  AND v.active = 1") ) {
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}

	public Set<Integer> bibsMarkedAsNeedingReindexingDueToDataChange() throws SQLException {
		Set<Integer> l = new HashSet<Integer>();
		try (   Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(
				"SELECT bib_id FROM "+CurrentDBTable.QUEUE
				+ " WHERE done_date = 0"
				+ " AND priority = "+IndexQueuePriority.DATACHANGE.ordinal()
				+ " AND cause != '"+DataChangeUpdateType.DELETE+"'") ){
			while (rs.next()) l.add(rs.getInt(1));
		}
		return l;
	}

	@SuppressWarnings("resource") // for preparedstatement
	private int getBibForMfhd( CurrentDBTable table, int mfhdId ) throws SQLException {
		String statementKey = "mfhd2bib_"+table.toString();
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

	
	@SuppressWarnings("resource") // for preparedstatement
	public void queueBibs(Set<Integer> bibsToAdd, DataChangeUpdateType type) throws Exception {
		if (bibsToAdd == null || bibsToAdd.isEmpty())
			return;
		if ( ! pstmts.containsKey("queueBib"))
			pstmts.put("queueBib", conn.prepareStatement(
					"INSERT INTO "+CurrentDBTable.QUEUE+
					" (bib_id, priority, cause) VALUES (?, 0, ?)"));
		PreparedStatement pstmt = pstmts.get("queueBib");
		pstmt.setString(2, type.toString());
		for (Integer bib : bibsToAdd) {
			if (bib == null || bib.equals(0)) continue;
			pstmt.setInt(1, bib);
			pstmt.addBatch();
		}
		pstmt.executeBatch();
	}


	public class ChangedBib {
		public int original;
		public int changed;
		public ChangedBib( int original, int changed ) {
			this.original = original;
			this.changed = changed;
		}
	}

}
