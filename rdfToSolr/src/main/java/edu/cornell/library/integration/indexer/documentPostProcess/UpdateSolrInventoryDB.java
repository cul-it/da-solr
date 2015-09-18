package edu.cornell.library.integration.indexer.documentPostProcess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
			// bib is new
			populateBibField( conn, document, bibid, bibDate );
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
		} else {
			// if (bibDate.after(origBibDate))  bib is changed
			removeOrigHoldingsDataFromDB( conn, document, bibid );
			updateBibField( conn, document, bibid, bibDate );
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
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
			if (  ! format.equals(origDescRS.getString(1))
					|| ! location.equals(origDescRS.getString(2))
					|| ! edition.equals(origDescRS.getString(3))
					|| ! pub_date.equals(origDescRS.getString(4)))
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

	private String calculateDisplayLocation(SolrInputDocument document) {
		List<String> locations = new ArrayList<String>();
		if (document.containsKey("url_access_display")) {
//			Collection<Object> urls = document.getFieldValues("url_access_display");
			locations.add("Online"); //TODO: detailed location labeling.
		}
		if (document.containsKey("location_facet")) {
			String libraries = fieldValuesToConcatString(document.getFieldValues("location_facet"));
			locations.add("At the Library: "+libraries);
		}
		return StringUtils.join(locations," / ");
	}

	private String fieldValuesToConcatString(Collection<Object> fieldValues) {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Object val : fieldValues) {
			if (first)
				first = false;
			else
				sb.append(',');
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
