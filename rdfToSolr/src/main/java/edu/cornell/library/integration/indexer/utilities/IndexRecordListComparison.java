package edu.cornell.library.integration.indexer.utilities;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
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

	private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	private Connection conn = null;
	private Map<String,PreparedStatement> pstmts = new HashMap<String,PreparedStatement>();

	
	public static List<String> requiredArgs() {
		List<String> l = new ArrayList<String>();
		l.addAll(SolrBuildConfig.getRequiredArgsForDB("Current"));
		l.add("solrUrl");
		return l;
	}

	public void compare(SolrBuildConfig config) throws Exception {

		URL queryUrl = new URL(config.getSolrUrl() +
				"/select?qt=standard&q=id%3A*&wt=csv&fl=bibid_display,holdings_display,item_display&rows=50000000");
		
	    String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
	    conn = config.getDatabaseConnection("Current");

	    // Save Solr data to a temporary file
	    final Path tempPath = Files.createTempFile("indexRecordListCompare-", ".csv");
		tempPath.toFile().deleteOnExit();
		FileOutputStream fos = new FileOutputStream(tempPath.toString());
		ReadableByteChannel rbc = Channels.newChannel(queryUrl.openStream());
		fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE); //Integer.MAX_VALUE translates to 2 gigs max download
		fos.close();
		
		String bibTable = "bib_"+today;
		String mfhdTable = "mfhd_"+today;
		String itemTable = "item_"+today;
		
		// Then read the file back in to process it
		BufferedReader reader = Files.newBufferedReader(tempPath , StandardCharsets.UTF_8);
		String line = null;
		while ((line = reader.readLine()) != null ) {
			if (line.startsWith("bibid_display")) continue;
			
			int holdings_index = line.indexOf(',');
			int bibid = processSolrBibData(bibTable, line.substring(0, holdings_index));
			if (holdings_index + 1 == line.length()) continue;
			
			int item_index;
			if (line.charAt(holdings_index + 1) == '"') {
				item_index = line.indexOf('"', holdings_index+2)+1;
				processSolrHoldingsData(mfhdTable, line.substring(holdings_index+2, item_index-1), bibid);
			} else {
				item_index = line.indexOf(',',holdings_index+1);
				processSolrHoldingsData(mfhdTable, line.substring(holdings_index+1, item_index), bibid);
			}
			if (item_index + 1 == line.length()) continue;
			if (line.charAt(item_index + 1) == '"') {
				processSolrItemData(itemTable,mfhdTable,line.substring(item_index + 2, line.length()-1),bibid);
			} else {
				processSolrItemData(itemTable,mfhdTable,line.substring(item_index + 1),bibid);
			}
		}
		
		// Review database tables for certain conditions
		Statement stmt = conn.createStatement();
		// updated bibs
		ResultSet rs = stmt.executeQuery(
				"SELECT bib_id FROM "+bibTable+" WHERE found_in_solr = 1 AND voyager_date > date_add(solr_date,interval 15 second)");
		while (rs.next()) bibsNewerInVoyagerThanIndex.add(rs.getInt(1));
		rs.close();
		// updated holdings
		rs = stmt.executeQuery(
				"SELECT mfhd_id,bib_id FROM "+mfhdTable+" WHERE found_in_solr = 1 AND voyager_date > date_add(solr_date,interval 15 second)");
		while (rs.next())
			mfhdsNewerInVoyagerThanIndex.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		// updated items
		rs = stmt.executeQuery(
				"SELECT item_id,mfhd_id FROM "+itemTable+" WHERE found_in_solr = 1 AND voyager_date > date_add(solr_date,interval 15 second)");
		while (rs.next())
			itemsNewerInVoyagerThanIndex.put(rs.getInt(1),getBibForMfhd(mfhdTable,rs.getInt(2)));
		rs.close();
		// new bibs
		rs = stmt.executeQuery(
				"SELECT bib_id FROM "+bibTable+" WHERE found_in_solr = 0");
		while (rs.next()) bibsInVoyagerNotIndex.add(rs.getInt(1));
		rs.close();
		// new holdings
		rs = stmt.executeQuery(
				"SELECT mfhd_id,bib_id FROM "+mfhdTable+" WHERE found_in_solr = 0");
		while (rs.next())
			mfhdsInVoyagerNotIndex.put(rs.getInt(1),rs.getInt(2));
		rs.close();
		// new items
		rs = stmt.executeQuery(
				"SELECT item_id,mfhd_id FROM "+itemTable+" WHERE found_in_solr = 0");
		while (rs.next())
			itemsInVoyagerNotIndex.put(rs.getInt(1),getBibForMfhd(mfhdTable,rs.getInt(2)));
		rs.close();
	}

	private int processSolrBibData(String bibTable, String solrBib) throws SQLException, ParseException {
		bibCount++;
		String[] parts = solrBib.split("\\|", 2);
		int bibid = Integer.valueOf(parts[0]);
		if ( ! pstmts.containsKey("bib_update"))
			pstmts.put("bib_update",conn.prepareStatement(
					"UPDATE "+bibTable+" SET solr_date = ? , found_in_solr = 1 "
							+ "WHERE bib_id = ?"));
		
		// Attempt to reflect Solr date in table. If fails, record not in Voyager.
		// note: date comparison comes later.
		PreparedStatement pstmt = pstmts.get("bib_update");
		pstmt.setTimestamp(1, new Timestamp( dateFormat.parse(parts[1]).getTime() ));
		pstmt.setInt(2, bibid);
		int rowCount = pstmt.executeUpdate();

		// bib is in Solr & Voyager
		if (rowCount == 1) return bibid;

		// bib is in Solr, not Voyager
		bibsInIndexNotVoyager.add(bibid);
		return bibid;
	}

	private void processSolrHoldingsData(String holdingsTable, String solrHoldings, int bibid) throws SQLException, ParseException {
		String[] solrHoldingsList = solrHoldings.split(",");
		if ( ! pstmts.containsKey("mfhd_update"))
			pstmts.put("mfhd_update",conn.prepareStatement(
					"UPDATE "+holdingsTable+" SET solr_date = ? , found_in_solr = 1 "
							+ "WHERE mfhd_id = ? AND bib_id = ? "));
		PreparedStatement pstmt = pstmts.get("mfhd_update");
		for (int i = 0; i < solrHoldingsList.length; i++) {
			int holdingsId;
			mfhdCount++;
			Timestamp modified = null;
			if (solrHoldingsList[i].contains("|")) {
				String[] parts = solrHoldingsList[i].split("\\|",2);
				holdingsId = Integer.valueOf(parts[0]);
				modified = new Timestamp( dateFormat.parse(parts[1]).getTime() );
			} else {
				holdingsId = Integer.valueOf(solrHoldingsList[i]);
			}
			pstmt.setTimestamp(1, modified);
			pstmt.setInt(2, holdingsId);
			pstmt.setInt(3, bibid);
			int rowCount = pstmt.executeUpdate();

			// mfhd is in Solr & Voyager
			if (rowCount == 1) continue;

			// If query didn't affect any rows, the holding may be missing from Voyager, or
			// it may have been reassigned to a different bib.
			int oldBibid = getBibForMfhd( holdingsTable, holdingsId);
			
			if (oldBibid == 0) {
				mfhdsInIndexNotVoyager.put(holdingsId, bibid);
			} else {
				mfhdsAttachedToDifferentBibs.put(holdingsId, new ChangedBib(oldBibid, bibid));
			}
			
		}
		
	}

	private void processSolrItemData(String itemTable, String holdingsTable, String solrItems, int bibid) throws SQLException, ParseException {
		String[] solrItemList = solrItems.split(",");
		if ( ! pstmts.containsKey("item_update"))
			pstmts.put("item_update", conn.prepareStatement(
					"UPDATE "+itemTable+" SET solr_date = ? , found_in_solr = 1 "
							+ "WHERE item_id = ? AND mfhd_id = ? "));
		PreparedStatement pstmt = pstmts.get("item_update");
		for (int i = 0; i < solrItemList.length; i++) {
			itemCount++;
			Timestamp modified = null;
			String[] parts = solrItemList[i].split("\\|");
			int itemId = Integer.valueOf(parts[0]);
			int mfhdId = Integer.valueOf(parts[1]);
			if (parts.length > 2)
				modified = new Timestamp( dateFormat.parse(parts[2]).getTime() );
			pstmt.setTimestamp(1, modified);
			pstmt.setInt(2, itemId);
			pstmt.setInt(3, mfhdId);
			int rowCount = pstmt.executeUpdate();

			// item is in Solr & Voyager
			if (rowCount == 1) continue;

			// If query didn't affect any rows, the item may be missing from Voyager, or
			// it may have been reassigned to a different holdings. In the latter case, it's
			// more important to identify the old bib than the old holdings.
			int oldBibid = getBibForItem( itemTable, holdingsTable, itemId );

			if (oldBibid == 0) {
				itemsInIndexNotVoyager.put(itemId, bibid);
			} else {
				// The item is attached to a different mfhd, but both mfhds may be attached
				// to the same bib. In that case, we record the same bib as old and new bib
				// which has the desired affect of triggering the bib for update in the index.
				// If the bibs differ, both should be queued for update.
				itemsAttachedToDifferentMfhds.put(itemId, new ChangedBib(oldBibid, bibid));
			}

		}
	}

	private int getBibForItem( String itemTable, String mfhdTable, int itemId ) throws SQLException {
		if ( ! pstmts.containsKey("item2mfhd"))
			pstmts.put("item2mfhd", conn.prepareStatement(
					"SELECT mfhd_id FROM "+itemTable+" WHERE item_id = ?"));
		PreparedStatement pstmt = pstmts.get("item2mfhd");
		pstmt.setInt(1, itemId);
		ResultSet rs = pstmt.executeQuery();
		int mfhdId = 0;
		while (rs.next())
			mfhdId = rs.getInt(1);
		rs.close();
		return getBibForMfhd(mfhdTable,mfhdId);
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
