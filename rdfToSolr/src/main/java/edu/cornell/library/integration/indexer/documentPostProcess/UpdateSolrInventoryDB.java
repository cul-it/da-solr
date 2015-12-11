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
	private final SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	private static String bibTable = null;
	private static String mfhdTable = null;
	private static String itemTable = null;
	private static String workTable = null;
	private static String solrIndexName = null;
	private static Connection conn = null;
	private static PreparedStatement 
			updateDescStmt = null,  updateIndexedDateStmt = null,
			origBibDateStmt = null, origDescStmt = null, origWorksStmt = null,   
			origMfhdStmt = null,
			insertBibStmt = null,  insertMfhdStmt = null,  insertItemStmt = null,
			findWorksForOclcIdStmt = null,
			insertBibWorkMappingStmt = null, updateBibWorkMappingStmt = null,
			deleteItemStmt = null, deleteMfhdStmt = null;

	private void setup(SolrBuildConfig config, SolrInputDocument document) throws Exception{
		
		conn = config.getDatabaseConnection("Current");
		String solrUrl = config.getSolrUrl();
		solrIndexName = solrUrl.substring(solrUrl.lastIndexOf('/')+1);
		bibTable = "bibSolr"+solrIndexName;
		mfhdTable = "mfhdSolr"+solrIndexName;
		itemTable = "itemSolr"+solrIndexName;
		workTable = "bib2work"+solrIndexName;
		origBibDateStmt = conn.prepareStatement("SELECT record_date FROM "+bibTable+" WHERE bib_id = ?");
		origMfhdStmt = conn.prepareStatement(
				"SELECT mfhd_id FROM "+mfhdTable+" WHERE bib_id = ?");
		origDescStmt = conn.prepareStatement(
				"SELECT format, location_label, edition, pub_date FROM "+bibTable+" WHERE bib_id = ?");
		updateDescStmt = conn.prepareStatement(
				"UPDATE "+bibTable
				+" SET record_date = ?, format = ?, location_label = ?, "
					+ "edition = ?, pub_date = ?, index_date = NOW(), linking_mod_date = NOW() "
				+ "WHERE bib_id = ?");
		updateIndexedDateStmt = conn.prepareStatement(
				"UPDATE "+bibTable
				+" SET record_date = ?, index_date = NOW() "
				+"WHERE bib_id = ?");
		origWorksStmt = conn.prepareStatement(
				"SELECT DISTINCT oclc_id FROM "+workTable+" WHERE bib_id = ?");
		insertBibStmt = conn.prepareStatement(
				"INSERT INTO "+bibTable+" (bib_id, record_date, format, location_label,index_date,edition,pub_date) "
						+ "VALUES (?, ?, ?, ?, NOW(), ?, ?)");
		insertMfhdStmt = conn.prepareStatement(
				"INSERT INTO "+mfhdTable+" (bib_id, mfhd_id, record_date) "
						+ "VALUES (?, ?, ?)");
		insertItemStmt = conn.prepareStatement(
				"INSERT INTO "+itemTable+" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)");
		findWorksForOclcIdStmt = conn.prepareStatement(
				"SELECT workid FROM workids.work2oclc WHERE oclcid = ?");
		insertBibWorkMappingStmt = conn.prepareStatement(
				"INSERT INTO "+workTable+" (bib_id, oclc_id, work_id) VALUES (?, ?, ?)");
		updateBibWorkMappingStmt = conn.prepareStatement(
				"UPDATE "+workTable+" SET active = 0, mod_date = NOW() WHERE bib_id = ? AND oclc_id = ?");
		deleteItemStmt = conn.prepareStatement(
				"DELETE FROM "+itemTable+" WHERE mfhd_id = ?");
		deleteMfhdStmt = conn.prepareStatement(
				"DELETE FROM "+mfhdTable+" WHERE bib_id = ?");
	}
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		if (conn == null || conn.isClosed())
			setup(config,document);

		// compare bib, mfhd and item list and dates to inventory, updating if need be
		String bibid_display = document.getField("bibid_display").getValue().toString();
		String[] tmp = bibid_display.split("\\|",2);
		Integer bibid = Integer.valueOf(tmp[0]);
		Timestamp bibDate = new Timestamp( marcDateFormat.parse(tmp[1]).getTime() );
		origBibDateStmt.setInt(1, bibid);
		ResultSet rs = origBibDateStmt.executeQuery();
		Timestamp origBibDate = null;
		while (rs.next()) {
			origBibDate = rs.getTimestamp(1);
		}
		rs.close();
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

		// pull existing values from DB are the descriptive fields changed?
		origDescStmt.setInt(1, bibid);
		ResultSet origDescRS = origDescStmt.executeQuery();
		boolean descChanged = false;
		while (origDescRS.next())
			if (  ! stringsEqual(format,origDescRS.getString(1))
					|| ! stringsEqual(location,origDescRS.getString(2))
					|| ! stringsEqual(edition,origDescRS.getString(3))
					|| ! stringsEqual(pub_date,origDescRS.getString(4)))
				descChanged = true;
		origDescRS.close();

		// update db appropriately
		if (descChanged) {
			updateDescStmt.setTimestamp(1, recordDate);
			updateDescStmt.setString(2, format);
			updateDescStmt.setString(3, location);
			updateDescStmt.setString(4, edition);
			updateDescStmt.setString(5, pub_date);
			updateDescStmt.setInt(6, bibid);
			updateDescStmt.executeUpdate();
		}
		if ( ! descChanged ) {
			updateIndexedDateStmt.setTimestamp(1, recordDate);
			updateIndexedDateStmt.setInt(2, bibid);
			updateIndexedDateStmt.executeUpdate();
		}
	}

	private void updateWorkInfo(Connection conn, SolrInputDocument document, int bibid) throws SQLException {

		Set<Integer> oclcIds = extractOclcIdsFromSolrField(
				document.getFieldValues("other_id_display"));
		Set<Integer> previousOclcIds = new HashSet<Integer>();
		origWorksStmt.setInt(1, bibid);
		ResultSet rs = origWorksStmt.executeQuery();
		while (rs.next()) {
			previousOclcIds.add(rs.getInt(1));
		}
		rs.close();

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
		insertBibStmt.setInt(1, bibid);
		insertBibStmt.setTimestamp(2, recordDate);
		insertBibStmt.setString(3, format);
		insertBibStmt.setString(4, location);
		insertBibStmt.setString(5, edition);
		insertBibStmt.setString(6, pub_date);
		insertBibStmt.executeUpdate();
	}
	private void populateHoldingFields(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		List<HoldingRecord> holdings = extractHoldingsFromSolrField(
				document.getFieldValues("holdings_display"));
		for (HoldingRecord holding : holdings ) {
			insertMfhdStmt.setInt(1, bibid);
			insertMfhdStmt.setInt(2, holding.id);
			insertMfhdStmt.setTimestamp(3, holding.modified);
			insertMfhdStmt.addBatch();
		}
		insertMfhdStmt.executeBatch();
	}

	private void populateItemFields(Connection conn, SolrInputDocument document) throws SQLException, ParseException {

		if ( ! document.containsKey("item_display") )
			return;
		List<ItemRecord> items = extractItemsFromSolrField(
				document.getFieldValues("item_display"));
		for (ItemRecord item : items ) {
			insertItemStmt.setInt(1, item.mfhdid);
			insertItemStmt.setInt(2, item.id);
			insertItemStmt.setTimestamp(3, item.modified);
			insertItemStmt.addBatch();
		}
		insertItemStmt.executeBatch();
	}

	private void populateWorkInfo(Connection conn, Set<Integer> oclcIds, int bibid) throws SQLException {

		for (int oclcId : oclcIds) {
			findWorksForOclcIdStmt.setInt(1, oclcId);
			ResultSet rs = findWorksForOclcIdStmt.executeQuery();
			while (rs.next()) {
				long workid = rs.getLong(1);
				insertBibWorkMappingStmt.setInt(1, bibid);
				insertBibWorkMappingStmt.setInt(2, oclcId);
				insertBibWorkMappingStmt.setLong(3, workid);
				insertBibWorkMappingStmt.addBatch();
			}
			rs.close();
		}
		insertBibWorkMappingStmt.executeBatch();

	}

	private void deactivateWorkInfo(Connection conn, Set<Integer> removedOclcIds, int bibid) throws SQLException {

		if (removedOclcIds.isEmpty()) 
			return;
		for (int oclcid : removedOclcIds) {
			updateBibWorkMappingStmt.setInt(1, bibid);
			updateBibWorkMappingStmt.setInt(2, oclcid);
			updateBibWorkMappingStmt.addBatch();
		}
		updateBibWorkMappingStmt.executeBatch();
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
		origMfhdStmt.setInt(1, bibid);
		ResultSet mfhdRs = origMfhdStmt.executeQuery();
		while (mfhdRs.next()) {
			deleteItemStmt.setInt(1, mfhdRs.getInt(1));
			deleteItemStmt.addBatch();
		}
		mfhdRs.close();
		deleteItemStmt.executeBatch();
		deleteMfhdStmt.setInt(1, bibid);
		deleteMfhdStmt.executeUpdate();
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
