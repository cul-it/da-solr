package edu.cornell.library.integration.indexer.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;


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

	private static String bibTableVoy;
	private static String mfhdTableVoy;
	private static String itemTableVoy;
	private static String bibTableSolr;
	private static String mfhdTableSolr;
	private static String itemTableSolr;


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

	    String solrUrl = config.getSolrUrl();
	    String solrCore = solrUrl.substring(solrUrl.lastIndexOf('/')+1);
		String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());

		bibTableVoy = "bib_"+today;
		mfhdTableVoy = "mfhd_"+today;
		itemTableVoy = "item_"+today;
		bibTableSolr = "bib_solr_"+solrCore;
		mfhdTableSolr = "mfhd_solr_"+solrCore;
		itemTableSolr = "item_solr_"+solrCore;

		conn = config.getDatabaseConnection("Current");
		stmt = conn.createStatement();

	}

	public Map<Integer,ChangedBib> mfhdsAttachedToDifferentBibs() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<Integer,ChangedBib>();
		ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id, s.bib_id "
				+ "FROM "+mfhdTableVoy+" as v,"+mfhdTableSolr + " as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND v.bib_id != s.bib_id");
		while (rs.next())
			m.put(rs.getInt(1), new ChangedBib(rs.getInt(3),rs.getInt(2)));
		rs.close();
		return m;
	}
	
	public Map<Integer,ChangedBib> itemsAttachedToDifferentMfhds() throws SQLException {
		Map<Integer,ChangedBib> m = new HashMap<Integer,ChangedBib>();
		ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id, s.mfhd_id "
				+ "FROM "+itemTableVoy+" as v,"+itemTableSolr + " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND v.mfhd_id != s.mfhd_id");
		while (rs.next())
			m.put(rs.getInt(1),
					new ChangedBib(getBibForMfhd(mfhdTableSolr,rs.getInt(3)),
							getBibForMfhd(mfhdTableVoy,rs.getInt(2))));
		rs.close();
		return m;
	}

	
	public Map<Integer,Integer> itemsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// new items
		ResultSet rs = stmt.executeQuery(
				"select v.item_id, v.mfhd_id from "+itemTableVoy+" as v "
				+ "left join "+itemTableSolr+" as s on s.item_id = v.item_id "
				+ "where solr_date is null");
		while (rs.next())
			m.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		return m;
	}

	
	public Map<Integer,Integer> itemsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// deleted items
		ResultSet rs = stmt.executeQuery(
				"select s.item_id, s.mfhd_id from "+itemTableSolr+" as s "
				+ "left join "+itemTableVoy+" as v on s.item_id = v.item_id "
				+ "where voyager_date is null");
		while (rs.next()) m.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		return m;
	}
	
	public Map<Integer,Integer> itemsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		ResultSet rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id "
				+ "FROM "+itemTableVoy+" as v,"+itemTableSolr + " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND (voyager_date > date_add(s.solr_date,interval 15 second) "
				+ "     OR ( voyager_date is not null AND s.solr_date is null))");
		while (rs.next())
			m.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		return m;
	}

	
	public Map<Integer,Integer> mfhdsInVoyagerNotIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		ResultSet rs = stmt.executeQuery(
				"select v.mfhd_id, v.bib_id from "+mfhdTableVoy+" as v "
				+ "left join "+mfhdTableSolr+" as s on s.mfhd_id = v.mfhd_id "
				+ "where solr_date is null");
		while (rs.next())
			m.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		return m;
	}
	
	public Map<Integer,Integer> mfhdsInIndexNotVoyager() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// deleted mfhds
		ResultSet rs = stmt.executeQuery(
				"select s.mfhd_id, s.bib_id from "+mfhdTableSolr+" as s "
				+ "left join "+mfhdTableVoy+" as v on s.mfhd_id = v.mfhd_id "
				+ "where voyager_date is null");
		while (rs.next()) m.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		return m;
	}
	
	public Map<Integer,Integer> mfhdsNewerInVoyagerThanIndex() throws SQLException {
		Map<Integer,Integer> m = new HashMap<Integer,Integer>();
		// updated holdings
		ResultSet rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id "
				+ "FROM "+mfhdTableVoy+" as v,"+mfhdTableSolr + " as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND (voyager_date > date_add(s.solr_date,interval 15 second) "
				+ "     OR ( voyager_date is not null AND s.solr_date is null))");
		while (rs.next())
			m.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		return m;
	}
	
	public Set<Integer> bibsInVoyagerNotIndex() throws SQLException {
		Set<Integer> l = new HashSet<Integer>();
		ResultSet rs = stmt.executeQuery(
				"select v.bib_id from "+bibTableVoy+" as v "
				+ "left join "+bibTableSolr+" as s on s.bib_id = v.bib_id "
				+ "where solr_date is null");
		while (rs.next()) l.add(rs.getInt(1));
		rs.close();
		return l;
	}
	
	public Set<Integer> bibsInIndexNotVoyager() throws SQLException {
		
		Set<Integer> l = new HashSet<Integer>();
		ResultSet rs = stmt.executeQuery(
				"select s.bib_id from "+bibTableSolr+" as s "
				+ "left join "+bibTableVoy+" as v on s.bib_id = v.bib_id "
				+ "where voyager_date is null");
		while (rs.next()) l.add(rs.getInt(1));
		rs.close();
		return l;
	}

	public Set<Integer> bibsNewerInVoyagerThanIndex() throws SQLException {
		Set<Integer> l = new HashSet<Integer>();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(
				"select v.bib_id from "+bibTableVoy+" as v, "+bibTableSolr+" as s "
				+ "WHERE v.bib_id = s.bib_id "
				+ "  AND voyager_date > date_add(solr_date,interval 15 second)");
		while (rs.next()) l.add(rs.getInt(1));
		rs.close();
		stmt.close();
		return l;
	}

	private int getBibForMfhd( String mfhdTable, int mfhdId ) throws SQLException {
		if ( ! pstmts.containsKey("mfhd2bib"))
			pstmts.put("mfhd2bib", conn.prepareStatement(
					"SELECT bib_id FROM "+mfhdTable+" WHERE mfhd_id = ?"));
		PreparedStatement pstmt = pstmts.get("mfhd2bib");
		pstmt.setInt(1, mfhdId);
		ResultSet rs = pstmt.executeQuery();
		int bibid = 0;
		while (rs.next())
			bibid = rs.getInt(1);
		rs.close();
		pstmt = null;
		return bibid;
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
