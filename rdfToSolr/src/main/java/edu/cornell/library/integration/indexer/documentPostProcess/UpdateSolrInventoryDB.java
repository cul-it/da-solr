package edu.cornell.library.integration.indexer.documentPostProcess;

import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.identifyOnlineServices;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/** Evaluate populated fields for conditions of membership for any collections.
 * (Currently only "Law Library".)
 *  */
public class UpdateSolrInventoryDB implements DocumentPostProcess{

	final static Boolean debug = false;
	private String bibTable = null;
	private String mfhdTable = null;
	private String itemTable = null;
	private String workTable = null;
	private final SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		Connection conn = config.getDatabaseConnection("Current");
		String solrUrl = config.getSolrUrl();
		String solrIndexName = solrUrl.substring(solrUrl.lastIndexOf('/')+1);
		bibTable = "bibSolr"+solrIndexName;
		mfhdTable = "mfhdSolr"+solrIndexName;
		itemTable = "itemSolr"+solrIndexName;
		workTable = "bib2work"+solrIndexName;

		// compare bib, mfhd and item list and dates to inventory, updating if need be
		String bibid_display = document.getField("bibid_display").getValue().toString();
		String[] tmp = bibid_display.split("\\|",2);
		Integer bibid = Integer.valueOf(tmp[0]);
		Timestamp bibDate = new Timestamp( marcDateFormat.parse(tmp[1]).getTime() );
		PreparedStatement pstmt = conn.prepareStatement("SELECT record_date FROM "+bibTable+" WHERE bib_id = ?");
		pstmt.setInt(1, bibid);
		ResultSet rs = pstmt.executeQuery();
		Timestamp origBibDate = null;
		while (rs.next()) {
			origBibDate = rs.getTimestamp(1);
		}
		if (origBibDate == null) {
			// bib is new in Solr
			populateBibField( conn, document, bibid, bibDate );
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
			populateWorkInfo( conn, extractOclcIdsFromSolrField(
					document.getFieldValues("other_id_display")), bibid );
		} else {
			removeOrigHoldingsDataFromDB( conn, document, bibid );
			updateBibField( conn, document, bibid, bibDate );
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
			updateWorkInfo( conn, document, bibid );
		}
		
	}

	private List<HoldingRecord> extractHoldingsFromSolrField(
			Collection<Object> fieldValues) throws ParseException {
		List<HoldingRecord> holdings = new ArrayList<HoldingRecord>();
		for (Object holding_obj : fieldValues) {
			String holding = holding_obj.toString();
			int mfhdid;
			Timestamp modified = null;
			if (holding.contains("|")) {
				String[] parts = holding.toString().split("\\|",2);
				mfhdid = Integer.valueOf(parts[0]);
				modified = new Timestamp( marcDateFormat.parse(parts[1]).getTime() );
			} else {
				mfhdid = Integer.valueOf(holding);
			}
			holdings.add( new HoldingRecord( mfhdid, modified ));
		}
		
		return holdings;
	}

	private List<ItemRecord> extractItemsFromSolrField(
			Collection<Object> fieldValues) throws ParseException {
		List<ItemRecord> items = new ArrayList<ItemRecord>();
		for (Object item_obj : fieldValues) {
			String item = item_obj.toString();
			Timestamp modified = null;
			String[] parts = item.split("\\|");
			int itemid = Integer.valueOf(parts[0]);
			int mfhdid = Integer.valueOf(parts[1]);
			if (parts.length > 2)
				modified = new Timestamp( marcDateFormat.parse(parts[2]).getTime() );
			items.add( new ItemRecord( itemid, mfhdid, modified ));
		}
		return items;
	}

	private Set<Integer> extractOclcIdsFromSolrField(
			Collection<Object> fieldValues) {
		Set<Integer> oclcIds = new HashSet<Integer>();
		for (Object id_obj : fieldValues) {
			String id = id_obj.toString();
			if (id.startsWith("(OCoLC)")) {
				try {
					oclcIds.add(Integer.valueOf(id.substring(7)));
				} catch (NumberFormatException e) {
					// Ignore the value if it's invalid
				}
			}
		}
		return oclcIds;
	}

	/* s1.equals(s2) will produce NPE if s1 is null. */
	private boolean stringsEqual( String s1, String s2 ) {
		if ( s1 == null ) {
			if ( s2 == null )
				return true;
		} else if ( s1.equals(s2) ) {
			return true;
		}
		return false;
	}

	private void updateBibField(Connection conn, SolrInputDocument document,
			Integer bibid, Timestamp recordDate) throws SQLException, ParseException {

		// generate necessary values
		String format = fieldValuesToConcatString(document.getFieldValues("format"));
		String edition = null;
		String pub_date = null;
		if (document.containsKey("pub_date_display"))
			pub_date = fieldValuesToConcatString(document.getFieldValues("pub_date_display"));
		if (document.containsKey("edition_display"))
			edition = fieldValuesToConcatString(document.getFieldValues("edition_display"));
		String location = calculateDisplayLocation( document );

		// pull existing values from DB
		PreparedStatement pstmt = conn.prepareStatement(
				"SELECT format, location_label, edition, pub_date FROM "+bibTable+" WHERE bib_id = ?");
		pstmt.setInt(1, bibid);
		ResultSet origDescRS = pstmt.executeQuery();

		// are the descriptive fields changed?
		boolean descChanged = false;
		while (origDescRS.next())
			if (  ! stringsEqual(format,origDescRS.getString(1))
					|| ! stringsEqual(location,origDescRS.getString(2))
					|| ! stringsEqual(edition,origDescRS.getString(3))
					|| ! stringsEqual(pub_date,origDescRS.getString(4)))
				descChanged = true;
		origDescRS.close();
		pstmt.close();

		// update db appropriately
		if (descChanged) {
			pstmt = conn.prepareStatement(
					"UPDATE "+bibTable
					+" SET record_date = ?, format = ?, location_label = ?, "
						+ "edition = ?, pub_date = ?, index_date = NOW(), linking_mod_date = NOW() "
					+ "WHERE bib_id = ?");
			pstmt.setTimestamp(1, recordDate);
			pstmt.setString(2, format);
			pstmt.setString(3, location);
			pstmt.setString(4, edition);
			pstmt.setString(5, pub_date);
			pstmt.setInt(6, bibid);
			pstmt.executeUpdate();
			pstmt.close();
		}
		if ( ! descChanged ) {
			pstmt = conn.prepareStatement(
					"UPDATE "+bibTable
					+" SET record_date = ?, index_date = NOW() "
					+"WHERE bib_id = ?");
			pstmt.setTimestamp(1, recordDate);
			pstmt.setInt(2, bibid);
			pstmt.executeUpdate();
			pstmt.close();
		}
	}

	private void updateWorkInfo(Connection conn, SolrInputDocument document, int bibid) throws SQLException {

		Set<Integer> oclcIds = extractOclcIdsFromSolrField(
				document.getFieldValues("other_id_display"));
		PreparedStatement previousWorksStmt = conn.prepareStatement(
				"SELECT DISTINCT oclcid FROM "+workTable+" WHERE bib_id = ?");
		Set<Integer> previousOclcIds = new HashSet<Integer>();
		previousWorksStmt.setInt(1, bibid);
		ResultSet rs = previousWorksStmt.executeQuery();
		while (rs.next()) {
			previousOclcIds.add(rs.getInt(1));
		}
		rs.close();
		previousWorksStmt.close();

		Set<Integer> newOclcIds = new HashSet<Integer>(oclcIds);
		newOclcIds.removeAll(previousOclcIds);
		populateWorkInfo(conn, newOclcIds, bibid);

		Set<Integer> removedOclcIds = new HashSet<Integer>(previousOclcIds);
		removedOclcIds.removeAll(oclcIds);
		deactivateWorkInfo(conn, removedOclcIds, bibid);

		// If we get updates for the workid database, we will also want to
		// do something with the carried over oclcids.

	}

	private void populateBibField(Connection conn, SolrInputDocument document,
			Integer bibid, Timestamp recordDate) throws SQLException, ParseException {
		String format = fieldValuesToConcatString(document.getFieldValues("format"));
		String edition = null;
		String pub_date = null;
		if (document.containsKey("pub_date_display"))
			pub_date = fieldValuesToConcatString(document.getFieldValues("pub_date_display"));
		if (document.containsKey("edition_display"))
			edition = fieldValuesToConcatString(document.getFieldValues("edition_display"));
		String location = calculateDisplayLocation( document );
		PreparedStatement pstmt = conn.prepareStatement(
				"INSERT INTO "+bibTable+" (bib_id, record_date, format, location_label,index_date,edition,pub_date) "
						+ "VALUES (?, ?, ?, ?, NOW(), ?, ?)");
		pstmt.setInt(1, bibid);
		pstmt.setTimestamp(2, recordDate);
		pstmt.setString(3, format);
		pstmt.setString(4, location);
		pstmt.setString(5, edition);
		pstmt.setString(6, pub_date);
		pstmt.executeUpdate();
		pstmt.close();
	}
	private void populateHoldingFields(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		List<HoldingRecord> holdings = extractHoldingsFromSolrField(
				document.getFieldValues("holdings_display"));
		PreparedStatement pstmt = conn.prepareStatement(
				"INSERT INTO "+mfhdTable+" (bib_id, mfhd_id, record_date) "
						+ "VALUES (?, ?, ?)");
		for (HoldingRecord holding : holdings ) {
			pstmt.setInt(1, bibid);
			pstmt.setInt(2, holding.id);
			pstmt.setTimestamp(3, holding.modified);
			pstmt.addBatch();
		}
		pstmt.executeBatch();
		pstmt.close();
	}

	private void populateItemFields(Connection conn, SolrInputDocument document) throws SQLException, ParseException {

		if ( ! document.containsKey("item_display") )
			return;
		List<ItemRecord> items = extractItemsFromSolrField(
				document.getFieldValues("item_display"));
		PreparedStatement pstmt = conn.prepareStatement(
				"INSERT INTO "+itemTable+" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)");
		for (ItemRecord item : items ) {
			pstmt.setInt(1, item.mfhdid);
			pstmt.setInt(2, item.id);
			pstmt.setTimestamp(3, item.modified);
			pstmt.addBatch();
		}
		pstmt.executeBatch();
		pstmt.close();
		
	}

	private void populateWorkInfo(Connection conn, Set<Integer> oclcIds, int bibid) throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement(
				"SELECT workid FROM work2oclc WHERE oclcid = ?");
		PreparedStatement insertStmt = conn.prepareStatement(
				"INSERT INTO "+workTable+" (bib_id, oclc_id, work_id) VALUES (?, ?, ?)");
		for (int oclcId : oclcIds) {
			pstmt.setInt(1, oclcId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				long workid = rs.getLong(1);
				insertStmt.setInt(1, bibid);
				insertStmt.setInt(2, oclcId);
				insertStmt.setLong(3, workid);
				insertStmt.addBatch();
			}
		}
		insertStmt.executeBatch();
		insertStmt.close();
		pstmt.close();

	}

	private void deactivateWorkInfo(Connection conn, Set<Integer> removedOclcIds, int bibid) throws SQLException {

		if (removedOclcIds.isEmpty()) 
			return;
		PreparedStatement pstmt = conn.prepareStatement(
				"UPDATE "+workTable+" SET active = 0, mod_date = NOW() WHERE bib_id = ? AND oclc_id = ?");
		for (int oclcid : removedOclcIds) {
			pstmt.setInt(1, bibid);
			pstmt.setInt(2, oclcid);
			pstmt.addBatch();
		}
		pstmt.executeBatch();
		pstmt.close();

	}

	private String calculateDisplayLocation(SolrInputDocument document) {
		List<String> locations = new ArrayList<String>();
		if (document.containsKey("url_access_display")) {
//			Collection<Object> urls = document.getFieldValues("url_access_display");
			String sites = identifyOnlineServices(document.getFieldValues("url_access_display"));
			if (sites == null)
				locations.add("Online");
			else
				locations.add("Online: "+sites);
		}
		if (document.containsKey("location_facet")) {
			String libraries = fieldValuesToConcatString(document.getFieldValues("location_facet"));
			locations.add("At the Library: "+libraries);
		}
		return StringUtils.join(locations," / ");
	}

	
	private String fieldValuesToConcatString(Collection<Object> fieldValues) {
		StringBuilder sb = new StringBuilder();
		Collection<Object> foundValues = new HashSet<Object>();
		boolean first = true;
		for (Object val : fieldValues) {
			if (foundValues.contains(val))
				continue;
			foundValues.add(val);
			if (first)
				first = false;
			else
				sb.append(", ");
			sb.append(val.toString());
		}
		return sb.toString();
	}

	private List<HoldingRecord> removeOrigHoldingsDataFromDB(
			Connection conn, SolrInputDocument document, int bibid) throws SQLException {
		List<HoldingRecord> holdings = new ArrayList<HoldingRecord>();
		PreparedStatement mfhdStmt = conn.prepareStatement(
				"SELECT mfhd_id FROM "+mfhdTable+" WHERE bib_id = ?");
		PreparedStatement itemDel = conn.prepareStatement(
				"DELETE FROM "+itemTable+" WHERE mfhd_id = ?");
		PreparedStatement mfhdDel = conn.prepareStatement(
				"DELETE FROM "+mfhdTable+" WHERE mfhd_id = ?");
		mfhdStmt.setInt(1, bibid);
		ResultSet mfhdRs = mfhdStmt.executeQuery();
		while (mfhdRs.next()) {
			int mfhdid = mfhdRs.getInt(1);
			mfhdDel.setInt(1, mfhdid);
			mfhdDel.addBatch();
			itemDel.setInt(1, mfhdid);
			itemDel.addBatch();
		}
		mfhdRs.close();
		mfhdStmt.close();
		itemDel.executeBatch();
		itemDel.close();
		mfhdDel.executeBatch();
		mfhdDel.close();
		return holdings;
	}

	private class HoldingRecord {
		int id;
		Timestamp modified;
		public HoldingRecord(int id, Timestamp modified) {
			this.id = id;
			this.modified = modified;
		}
	}
	private class ItemRecord {
		int id;
		int mfhdid;
		Timestamp modified;
		public ItemRecord(int id, int mfhdid, Timestamp modified) {
			this.id = id;
			this.mfhdid = mfhdid;
			this.modified = modified;
		}
	}


}
