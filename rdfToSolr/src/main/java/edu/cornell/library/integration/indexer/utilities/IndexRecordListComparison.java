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
    
	public Set<Integer> bibsInIndexNotVoyager = new HashSet<Integer>();
	public Set<Integer> bibsInVoyagerNotIndex = new HashSet<Integer>();
	public Set<Integer> bibsNewerInVoyagerThanIndex = new HashSet<Integer>();
	public Map<Integer,Integer> mfhdsInIndexNotVoyager = new HashMap<Integer,Integer>();
	public Map<Integer,Integer> mfhdsInVoyagerNotIndex = new HashMap<Integer,Integer>();
	public Map<Integer,Integer> mfhdsNewerInVoyagerThanIndex = new HashMap<Integer,Integer>();
	public Map<Integer,ChangedBib> mfhdsAttachedToDifferentBibs = new HashMap<Integer,ChangedBib>();
	public Map<Integer,Integer> itemsInIndexNotVoyager = new HashMap<Integer,Integer>();
	public Map<Integer,Integer> itemsInVoyagerNotIndex = new HashMap<Integer,Integer>();
	public Map<Integer,Integer> itemsNewerInVoyagerThanIndex = new HashMap<Integer,Integer>();
	public Map<Integer,ChangedBib> itemsAttachedToDifferentMfhds = new HashMap<Integer,ChangedBib>();
	public int bibCount = 0;
	public int mfhdCount = 0;
	public int itemCount = 0;

	private Connection conn = null;
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();

	
	public static List<String> requiredArgs() {
		List<String> l = new ArrayList<String>();
		l.addAll(SolrBuildConfig.getRequiredArgsForDB("Current"));
		l.add("solrUrl");
		return l;
	}

	public void compare(SolrBuildConfig config) throws Exception {
		
	    String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
	    conn = config.getDatabaseConnection("Current");
	    String solrUrl = config.getSolrUrl();
	    String solrCore = solrUrl.substring(solrUrl.lastIndexOf('/')+1);
		
		String bibTableVoy = "bib_"+today;
		String mfhdTableVoy = "mfhd_"+today;
		String itemTableVoy = "item_"+today;
		String bibTableSolr = "bib_solr_"+solrCore;
		String mfhdTableSolr = "mfhd_solr_"+solrCore;
		String itemTableSolr = "item_solr_"+solrCore;
		
		// Review database tables for certain conditions
		Statement stmt = conn.createStatement();
		// updated bibs
		ResultSet rs = stmt.executeQuery(
				"select v.bib_id from "+bibTableVoy+" as v, "+bibTableSolr+" as s "
				+ "WHERE v.bib_id = s.bib_id "
				+ "  AND voyager_date > date_add(solr_date,interval 15 second)");
		while (rs.next()) bibsNewerInVoyagerThanIndex.add(rs.getInt(1));
		rs.close();
		// updated holdings
		rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id "
				+ "FROM "+mfhdTableVoy+" as v,"+mfhdTableSolr + " as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND (voyager_date > date_add(s.solr_date,interval 15 second) "
				+ "     OR ( voyager_date is not null AND s.solr_date is null))");
		while (rs.next())
			mfhdsNewerInVoyagerThanIndex.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		// updated items
		rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id "
				+ "FROM "+itemTableVoy+" as v,"+itemTableSolr + " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND (voyager_date > date_add(s.solr_date,interval 15 second) "
				+ "     OR ( voyager_date is not null AND s.solr_date is null))");
		while (rs.next())
			itemsNewerInVoyagerThanIndex.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		// new bibs
		rs = stmt.executeQuery(
				"select v.bib_id from "+bibTableVoy+" as v "
				+ "left join "+bibTableSolr+" as s on s.bib_id = v.bib_id "
				+ "where solr_date is null");
		while (rs.next()) bibsInVoyagerNotIndex.add(rs.getInt(1));
		rs.close();
		// new holdings
		rs = stmt.executeQuery(
				"select v.mfhd_id from "+mfhdTableVoy+" as v "
				+ "left join "+mfhdTableSolr+" as s on s.mfhd_id = v.mfhd_id "
				+ "where solr_date is null");
		while (rs.next())
			mfhdsInVoyagerNotIndex.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		// new items
		rs = stmt.executeQuery(
				"select v.item_id from "+itemTableVoy+" as v "
				+ "left join "+itemTableSolr+" as s on s.item_id = v.item_id "
				+ "where solr_date is null");
		while (rs.next())
			itemsInVoyagerNotIndex.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		// deleted bibs
		rs = stmt.executeQuery(
				"select s.bib_id from "+bibTableSolr+" as s "
				+ "left join "+bibTableVoy+" as v on s.bib_id = v.bib_id "
				+ "where voyager_date is null");
		while (rs.next()) bibsInIndexNotVoyager.add(rs.getInt(1));
		rs.close();
		// deleted mfhds
		rs = stmt.executeQuery(
				"select s.mfhd_id, s.bib_id from "+mfhdTableSolr+" as s "
				+ "left join "+mfhdTableVoy+" as v on s.mfhd_id = v.mfhd_id "
				+ "where voyager_date is null");
		while (rs.next()) mfhdsInIndexNotVoyager.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		// deleted items
		rs = stmt.executeQuery(
				"select s.item_id, s.mfhd_id from "+itemTableSolr+" as s "
				+ "left join "+itemTableVoy+" as v on s.item_id = v.item_id "
				+ "where voyager_date is null");
		while (rs.next()) mfhdsInIndexNotVoyager.put(rs.getInt(1),getBibForMfhd(mfhdTableVoy,rs.getInt(2)));
		rs.close();
		// reassigned holdings
		rs = stmt.executeQuery(
				"SELECT v.mfhd_id, v.bib_id, s.bib_id "
				+ "FROM "+mfhdTableVoy+" as v,"+mfhdTableSolr + " as s "
				+"WHERE v.mfhd_id = s.mfhd_id "
				+ " AND v.bib_id != s.bib_id");
		while (rs.next())
			mfhdsAttachedToDifferentBibs.put(rs.getInt(1), new ChangedBib(rs.getInt(3),rs.getInt(2)));
		rs.close();
		// reassigned items
		rs = stmt.executeQuery(
				"SELECT v.item_id, v.mfhd_id, s.mfhd_id "
				+ "FROM "+itemTableVoy+" as v,"+itemTableSolr + " as s "
				+"WHERE v.item_id = s.item_id "
				+ " AND v.mfhd_id != s.mfhd_id");
		while (rs.next())
			mfhdsAttachedToDifferentBibs.put(rs.getInt(1),
					new ChangedBib(getBibForMfhd(mfhdTableSolr,rs.getInt(3)),
							getBibForMfhd(mfhdTableVoy,rs.getInt(2))));
		rs.close();
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
