package edu.cornell.library.integration.indexer.documentPostProcess;

import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.pullReferenceFields;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.indexer.utilities.IndexingUtilities.TitleMatchReference;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

/** Evaluate populated fields for conditions of membership for any collections.
 * (Currently only "Law Library".)
 *  */
public class UpdateSolrInventoryDB implements DocumentPostProcess{

	final static Boolean debug = false;
	private final SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	private Set<Long> workids = new HashSet<Long>();
	static ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		Connection conn = config.getDatabaseConnection("Current");

		// compare bib, mfhd and item list and dates to inventory, updating if need be
		String bibid_display = document.getField("bibid_display").getValue().toString();
		String[] tmp = bibid_display.split("\\|",2);
		Integer bibid = Integer.valueOf(tmp[0]);
		PreparedStatement origBibDateStmt = conn.prepareStatement(
				"SELECT record_date FROM "+CurrentDBTable.BIB_SOLR.toString()
				+" WHERE bib_id = ?");
		origBibDateStmt.setInt(1, bibid);
		ResultSet rs = origBibDateStmt.executeQuery();
		Timestamp origBibDate = null;
		while (rs.next()) {
			origBibDate = rs.getTimestamp(1);
		}
		rs.close();
		origBibDateStmt.close();
		Boolean knockOnUpdatesNeeded;
		if (origBibDate == null) {
			// bib is new in Solr
			populateBibField( conn, document );
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
			populateWorkInfo( conn, extractOclcIdsFromSolrField(
					document.getFieldValues("other_id_display")), bibid );
			knockOnUpdatesNeeded = true;
		} else {
			removeOrigHoldingsDataFromDB( conn, document, bibid );
			knockOnUpdatesNeeded = updateBibField( conn, document, bibid);
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
			updateWorkInfo( conn, document, bibid );
		}

		addWorkIdLinksToDocument(conn,document,bibid,knockOnUpdatesNeeded);
		conn.close();
	}

	private void addWorkIdLinksToDocument(Connection conn,
			SolrInputDocument document, int bibid, boolean knockOnUpdatesNeeded) throws Exception {
		if (workids.isEmpty())
			return;
		if (workids.size() > 1)
			System.out.println("bib with multiple associated works: "+bibid);
		
		PreparedStatement recordsForWorkStmt = conn.prepareStatement(
				"SELECT bib.* FROM "+CurrentDBTable.BIB_SOLR.toString()+" AS bib, "
						+CurrentDBTable.BIB2WORK.toString()+" AS works "
				+ "WHERE works.bib_id = bib.bib_id "
				+ "AND works.work_id = ? "
				+ "AND bib.active = 1 AND works.active = 1 ");
		PreparedStatement markBibForUpdateStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.QUEUE.toString()
				+ " (bib_id, priority, cause) VALUES"
				+ " (?, 0, '"+DataChangeUpdateType.TITLELINK.toString()+"')");
		Map<Integer,TitleMatchReference> refs = new HashMap<Integer,TitleMatchReference>();
		TitleMatchReference thisTitle = null;
		SolrInputField workidDisplay = new SolrInputField("workid_display");
		SolrInputField workidFacet = new SolrInputField("workid_facet");
		SolrInputField otherAvailPiped = new SolrInputField("other_availability_piped");
		SolrInputField otherAvailJson = new SolrInputField("other_availability_json");
		for (long workid : workids) {
			workidFacet.addValue(workid, 1);
			recordsForWorkStmt.setLong(1, workid);
			ResultSet rs = recordsForWorkStmt.executeQuery();
			int referenceCount = 0;
			while (rs.next()) {
				int refBibid = rs.getInt("bib_id");
				if (bibid == refBibid) {
					if (thisTitle == null) {
						thisTitle = new TitleMatchReference();
						thisTitle.id = bibid;
						thisTitle.format = rs.getString("format");
						thisTitle.sites = rs.getString("sites");
						thisTitle.libraries = rs.getString("libraries");
						thisTitle.edition = rs.getString("edition");
						thisTitle.pub_date = rs.getString("pub_date");
						thisTitle.language = rs.getString("language");
						thisTitle.title = rs.getString("title");
					}
				} else {
					if ( ! refs.containsKey(refBibid) ) {
						TitleMatchReference ref = new TitleMatchReference();
						ref.id = refBibid;
						ref.format = rs.getString("format");
						ref.sites = rs.getString("sites");
						ref.libraries = rs.getString("libraries");
						ref.edition = rs.getString("edition");
						ref.pub_date = rs.getString("pub_date");
						ref.language = rs.getString("language");
						ref.title = rs.getString("title");
						refs.put(refBibid, ref);
					}
					referenceCount++;
					if (knockOnUpdatesNeeded) {
						markBibForUpdateStmt.setInt(1,refBibid);
						markBibForUpdateStmt.addBatch();
					}
				}
			}
			if (referenceCount >= 1)
				workidDisplay.addValue(String.valueOf(workid)+"|"
						+String.valueOf(referenceCount+1), 1);
			rs.close();
		}
		recordsForWorkStmt.close();
		markBibForUpdateStmt.executeBatch();
		markBibForUpdateStmt.close();

		document.put("workid_facet", workidFacet);

		if (refs.isEmpty())
			return;
		if ( workidDisplay.getValueCount() > 0 )
			document.put("workid_display", workidDisplay);
		if (refs.size() <= 12) {
			for (Map.Entry<Integer,TitleMatchReference> ref : refs.entrySet()) {
				otherAvailPiped.addValue(generatePipedAvailability(thisTitle,ref.getValue()), 1);
				otherAvailJson.addValue(generateJsonAvailability(ref.getValue()), 1);
			}
			document.put("other_availability_piped", otherAvailPiped);
			document.put("other_availability_json", otherAvailJson);
		}
		
	}
	private String generateJsonAvailability(
			TitleMatchReference ref)  throws Exception {
		Map<String,Object> json = new HashMap<String,Object>();
		json.put("bibid", ref.id);
		json.put("format", ref.format);
		if (ref.sites != null && ! ref.sites.isEmpty())
			json.put("sites", ref.sites);
		if (ref.libraries != null && ! ref.libraries.isEmpty())
			json.put("libraries", ref.libraries);
		if (ref.edition != null && ! ref.edition.isEmpty())
			json.put("edition", ref.edition);
		if (ref.pub_date != null && ! ref.pub_date.isEmpty())
			json.put("pub_date", ref.pub_date);
		if (ref.language != null && ! ref.language.isEmpty())
			json.put("language", ref.language);
		if (ref.title != null && ! ref.title.isEmpty())
			json.put("title", ref.title);
		ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
		mapper.writeValue(jsonstream, json);
		return jsonstream.toString("UTF-8");
	}

	private String generatePipedAvailability(
			TitleMatchReference thisTitle,
			TitleMatchReference ref) {
		StringBuilder sb = new StringBuilder();
		if ( ! stringsEqual(thisTitle.format,ref.format)) {
			sb.append(ref.format);
			sb.append(": ");
		}
		boolean online = false;
		if (ref.sites != null && ! ref.sites.isEmpty()) {
			if (! ref.sites.equals("Online"))
				sb.append("Online: ");
			sb.append(ref.sites);		
			online = true;
		}
		if (ref.libraries != null && ! ref.libraries.isEmpty()) {
			if (online)
				sb.append(" / ");
			sb.append("At the Library: ");
			sb.append(ref.libraries);
		}
		String ed = ref.edition;
		if (ed != null && ! ed.isEmpty()) {
			sb.append(' ');
			sb.append(ed);
		}
		String date = ref.pub_date;
		if (date != null && ! date.isEmpty()) {
			sb.append(' ');
			sb.append(date);
		}
		return String.valueOf(ref.id)+"|"+sb.toString();
		
	}

	private List<HoldingRecord> extractHoldingsFromSolrField(
			Collection<Object> fieldValues) throws ParseException {
		List<HoldingRecord> holdings = new ArrayList<HoldingRecord>();
		if (fieldValues == null) return holdings;
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
		if (fieldValues == null) return oclcIds;
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
			if ( s2 == null || s2.isEmpty() )
				return true;
		} else if ( s2 == null ) {
			if ( s1.isEmpty() )
				return true;
		} else if ( s1.equals(s2) ) {
			return true;
		}
		return false;
	}

	private Boolean updateBibField(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		TitleMatchReference ref = pullReferenceFields(ClientUtils.toSolrDocument(document));

		// pull existing values from DB are the descriptive fields changed?
		PreparedStatement origDescStmt = conn.prepareStatement(
				"SELECT format, sites, libraries, edition,"
				+ " pub_date, title, language, active"
				+ " FROM "+CurrentDBTable.BIB_SOLR.toString()+" WHERE bib_id = ?");
		origDescStmt.setInt(1, bibid);
		ResultSet origDescRS = origDescStmt.executeQuery();
		boolean descChanged = false;
		while (origDescRS.next())
			if (  ! stringsEqual(ref.format,origDescRS.getString("format"))
					|| ! stringsEqual(ref.sites,origDescRS.getString("sites"))
					|| ! stringsEqual(ref.libraries,origDescRS.getString("libraries"))
					|| ! stringsEqual(ref.edition,origDescRS.getString("edition"))
					|| ! stringsEqual(ref.pub_date,origDescRS.getString("pub_date"))
					|| ! stringsEqual(ref.title,origDescRS.getString("title"))
					|| ! stringsEqual(ref.language,origDescRS.getString("language"))
					|| ! origDescRS.getBoolean("active"))
				descChanged = true;
		origDescRS.close();
		origDescStmt.close();

		// update db appropriately
		if (descChanged) {
			PreparedStatement updateDescStmt = conn.prepareStatement(
					"UPDATE "+CurrentDBTable.BIB_SOLR.toString()
					+" SET record_date = ?, format = ?, "
					+ "sites = ?, libraries = ?, "
					+ "edition = ?, pub_date = ?, title = ?, language = ?, "
					+ "index_date = NOW(), linking_mod_date = NOW(), "
					+ "active = 1 "
					+ "WHERE bib_id = ?");
			updateDescStmt.setTimestamp(1, ref.timestamp);
			updateDescStmt.setString(2, ref.format);
			updateDescStmt.setString(3, ref.sites);
			updateDescStmt.setString(4, ref.libraries);
			updateDescStmt.setString(5, ref.edition);
			updateDescStmt.setString(6, ref.pub_date);
			updateDescStmt.setString(7, ref.title);
			updateDescStmt.setString(8, ref.language);
			updateDescStmt.setInt(9, bibid);
			updateDescStmt.executeUpdate();
			updateDescStmt.close();
		}
		if ( ! descChanged ) {
			PreparedStatement updateIndexedDateStmt = conn.prepareStatement(
					"UPDATE "+CurrentDBTable.BIB_SOLR.toString()
					+" SET record_date = ?, index_date = NOW(), active = 1 "
					+"WHERE bib_id = ?");
			updateIndexedDateStmt.setTimestamp(1, ref.timestamp);
			updateIndexedDateStmt.setInt(2, bibid);
			updateIndexedDateStmt.executeUpdate();
			updateIndexedDateStmt.close();
		}
		PreparedStatement markDoneInQueueStmt = conn.prepareStatement(
				"UPDATE "+CurrentDBTable.QUEUE.toString()+" SET done_date = NOW()"
						+ " WHERE bib_id = ? AND NOT done_date");
		markDoneInQueueStmt.setInt(1, bibid);
		markDoneInQueueStmt.executeUpdate();
		markDoneInQueueStmt.close();
		
		return descChanged;
	}

	private void updateWorkInfo(Connection conn, SolrInputDocument document, 
			int bibid) throws SQLException {

		// While a non-Catalog record may have an OCLC id, we don't want to
		// use it for the work id mapping, as that is a catalog function.
		Set<Integer> oclcIds = null;
		if (document.getFieldValue("type").toString().equals("Catalog")) {
			oclcIds = extractOclcIdsFromSolrField(
					document.getFieldValues("other_id_display"));
		} else {
			oclcIds = new HashSet<Integer>();
		}
		Set<Integer> previousOclcIds = new HashSet<Integer>();
		PreparedStatement origWorksStmt = conn.prepareStatement(
				"SELECT DISTINCT oclc_id FROM "+CurrentDBTable.BIB2WORK.toString()+
				" WHERE bib_id = ? AND active");
		origWorksStmt.setInt(1, bibid);
		ResultSet rs = origWorksStmt.executeQuery();
		while (rs.next()) {
			previousOclcIds.add(rs.getInt(1));
		}
		rs.close();
		origWorksStmt.close();

		// new
		Set<Integer> newOclcIds = new HashSet<Integer>(oclcIds);
		newOclcIds.removeAll(previousOclcIds);
		populateWorkInfo(conn, newOclcIds, bibid);

		// removed
		Set<Integer> removedOclcIds = new HashSet<Integer>(previousOclcIds);
		removedOclcIds.removeAll(oclcIds);
		deactivateWorkInfo(conn, removedOclcIds, bibid);

		// retained
		oclcIds.retainAll(previousOclcIds);
		identifyRetainedWorkIds(conn, oclcIds);
	}

	private void populateBibField(Connection conn, SolrInputDocument document) throws SQLException, ParseException {
		TitleMatchReference ref = pullReferenceFields(ClientUtils.toSolrDocument(document));

		PreparedStatement insertBibStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.BIB_SOLR.toString()+
				" (bib_id, record_date, format, sites, libraries, "
				+ "index_date, edition, pub_date, title, language) "
						+ "VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?)");
		insertBibStmt.setInt(1, ref.id);
		insertBibStmt.setTimestamp(2, ref.timestamp);
		insertBibStmt.setString(3, ref.format);
		insertBibStmt.setString(4, ref.sites);
		insertBibStmt.setString(5, ref.libraries);
		insertBibStmt.setString(6, ref.edition);
		insertBibStmt.setString(7, ref.pub_date);
		insertBibStmt.setString(8, ref.title);
		insertBibStmt.setString(9, ref.language);
		insertBibStmt.executeUpdate();
		insertBibStmt.close();
	}
	private void populateHoldingFields(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		List<HoldingRecord> holdings = extractHoldingsFromSolrField(
				document.getFieldValues("holdings_display"));
		PreparedStatement insertMfhdStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.MFHD_SOLR.toString()+" (bib_id, mfhd_id, record_date) "
						+ "VALUES (?, ?, ?)");
		for (HoldingRecord holding : holdings ) {
			insertMfhdStmt.setInt(1, bibid);
			insertMfhdStmt.setInt(2, holding.id);
			insertMfhdStmt.setTimestamp(3, holding.modified);
			insertMfhdStmt.addBatch();
		}
		insertMfhdStmt.executeBatch();
		insertMfhdStmt.close();
	}

	private void populateItemFields(Connection conn, SolrInputDocument document) throws SQLException, ParseException {

		if ( ! document.containsKey("item_display") )
			return;
		List<ItemRecord> items = extractItemsFromSolrField(
				document.getFieldValues("item_display"));
		PreparedStatement insertItemStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.ITEM_SOLR.toString()+
				" (mfhd_id, item_id, record_date) VALUES (?, ?, ?)");
		for (ItemRecord item : items ) {
			insertItemStmt.setInt(1, item.mfhdid);
			insertItemStmt.setInt(2, item.id);
			insertItemStmt.setTimestamp(3, item.modified);
			insertItemStmt.addBatch();
		}
		insertItemStmt.executeBatch();
		insertItemStmt.close();
	}

	private void identifyRetainedWorkIds(Connection conn, Set<Integer> oclcIds) throws SQLException {

		PreparedStatement findWorksForOclcIdStmt = conn.prepareStatement(
				"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?");

		for (int oclcId : oclcIds) {
			findWorksForOclcIdStmt.setInt(1, oclcId);
			ResultSet rs = findWorksForOclcIdStmt.executeQuery();
			while (rs.next())
				workids.add(rs.getLong(1));
			rs.close();
		}
		findWorksForOclcIdStmt.close();
	}
	private void populateWorkInfo(Connection conn, Set<Integer> oclcIds, int bibid) throws SQLException {

		PreparedStatement findWorksForOclcIdStmt = conn.prepareStatement(
				"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?");
		PreparedStatement checkForDeactivatedBibWorkMappingStmt = conn.prepareStatement(
				"SELECT * FROM "+CurrentDBTable.BIB2WORK+
				" WHERE bib_id = ? AND oclc_id = ? AND work_id = ?");
		PreparedStatement activateBibWorkMappingStmt = conn.prepareStatement(
				"UPDATE "+CurrentDBTable.BIB2WORK+
				"  SET active = 1, mod_date = NOW() "+
				" WHERE bib_id = ? AND oclc_id = ? AND work_id = ?");
		PreparedStatement insertBibWorkMappingStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.BIB2WORK.toString()+
				" (bib_id, oclc_id, work_id) VALUES (?, ?, ?)");
		PreparedStatement findBibsForAddedWorksStmt = conn.prepareStatement(
				"SELECT bib_id"
				+ " FROM "+CurrentDBTable.BIB2WORK.toString()
				+" WHERE work_id = ?"
				+ "  AND oclc_id = ?"
				+ "  AND bib_id != ?"
				+ "  AND active");
		PreparedStatement markBibForUpdateStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.QUEUE.toString()
				+ " (bib_id, priority, cause) VALUES"
				+ " (?, 0, '"+DataChangeUpdateType.TITLELINK.toString()+"')");

		for (int oclcId : oclcIds) {
			findWorksForOclcIdStmt.setInt(1, oclcId);
			ResultSet rs = findWorksForOclcIdStmt.executeQuery();
			while (rs.next()) {
				long workid = rs.getLong(1);
				// Is the mapping already present, but deactivated?
				checkForDeactivatedBibWorkMappingStmt.setInt(1, bibid);
				checkForDeactivatedBibWorkMappingStmt.setInt(2, oclcId);
				checkForDeactivatedBibWorkMappingStmt.setLong(3, workid);
				ResultSet isDeactivatedRS = checkForDeactivatedBibWorkMappingStmt.executeQuery();
				boolean isDeactivated = false;
				while (isDeactivatedRS.next())
					isDeactivated = true;
				isDeactivatedRS.close();

				if (isDeactivated) {
					// If it's present but deactivated, then "adding" it means activating it.
					activateBibWorkMappingStmt.setInt(1, bibid);
					activateBibWorkMappingStmt.setInt(2, oclcId);
					activateBibWorkMappingStmt.setLong(3, workid);
					activateBibWorkMappingStmt.addBatch();
				} else {
					// If it's not present at all, then adding it involves an insert.
					insertBibWorkMappingStmt.setInt(1, bibid);
					insertBibWorkMappingStmt.setInt(2, oclcId);
					insertBibWorkMappingStmt.setLong(3, workid);
					insertBibWorkMappingStmt.addBatch();
				}
				workids.add(workid);
				findBibsForAddedWorksStmt.setLong(1, workid);
				findBibsForAddedWorksStmt.setInt(2, oclcId);
				findBibsForAddedWorksStmt.setInt(3, bibid);
				ResultSet knockOnRS = findBibsForAddedWorksStmt.executeQuery();
				while (knockOnRS.next()) {
					markBibForUpdateStmt.setInt(1, knockOnRS.getInt(1));
					markBibForUpdateStmt.addBatch();
				}
				knockOnRS.close();
			}
			rs.close();
		}
		findWorksForOclcIdStmt.close();
		insertBibWorkMappingStmt.executeBatch();
		insertBibWorkMappingStmt.close();
		activateBibWorkMappingStmt.executeBatch();
		activateBibWorkMappingStmt.close();
		markBibForUpdateStmt.executeBatch();
		markBibForUpdateStmt.close();
		findBibsForAddedWorksStmt.close();
	}

	private void deactivateWorkInfo(Connection conn, Set<Integer> removedOclcIds, int bibid) throws SQLException {

		if (removedOclcIds.isEmpty()) 
			return;
		PreparedStatement updateBibWorkMappingStmt = conn.prepareStatement(
				"UPDATE "+CurrentDBTable.BIB2WORK.toString()+
				" SET active = 0, mod_date = NOW() WHERE bib_id = ? AND oclc_id = ?");
		PreparedStatement findBibsForDeactivatedWorksStmt = conn.prepareStatement(
				"SELECT distinct b.bib_id"
				+ " FROM "+CurrentDBTable.BIB2WORK.toString()+" as a, "
						+CurrentDBTable.BIB2WORK.toString()+" as b"
				+ " WHERE a.work_id = b.work_id"
				+ "   AND a.bib_id = ? AND a.oclc_id = ?"
				+ "   AND b.bib_id != a.bib_id"
				+ "   AND b.active");
		PreparedStatement markBibForUpdateStmt = conn.prepareStatement(
				"INSERT INTO "+CurrentDBTable.QUEUE.toString()
				+ " (bib_id, priority, cause) VALUES"
				+ " (?, 0, '"+DataChangeUpdateType.TITLELINK.toString()+"')");
		for (int oclcid : removedOclcIds) {
			updateBibWorkMappingStmt.setInt(1, bibid);
			updateBibWorkMappingStmt.setInt(2, oclcid);
			updateBibWorkMappingStmt.addBatch();
			findBibsForDeactivatedWorksStmt.setInt(1, bibid);
			findBibsForDeactivatedWorksStmt.setInt(2, oclcid);
			ResultSet rs = findBibsForDeactivatedWorksStmt.executeQuery();
			while (rs.next()) {
				markBibForUpdateStmt.setInt(1, rs.getInt(1));
				markBibForUpdateStmt.addBatch();
			}
			rs.close();
		}
		updateBibWorkMappingStmt.executeBatch();
		updateBibWorkMappingStmt.close();
		markBibForUpdateStmt.executeBatch();
		markBibForUpdateStmt.close();
		findBibsForDeactivatedWorksStmt.close();
	}

	private List<HoldingRecord> removeOrigHoldingsDataFromDB(
			Connection conn, SolrInputDocument document, int bibid) throws SQLException {
		List<HoldingRecord> holdings = new ArrayList<HoldingRecord>();
		PreparedStatement origMfhdStmt = conn.prepareStatement(
				"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_SOLR.toString()+" WHERE bib_id = ?");
		PreparedStatement deleteItemStmt = conn.prepareStatement(
				"DELETE FROM "+CurrentDBTable.ITEM_SOLR.toString()+" WHERE mfhd_id = ?");
		origMfhdStmt.setInt(1, bibid);
		ResultSet mfhdRs = origMfhdStmt.executeQuery();
		while (mfhdRs.next()) {
			deleteItemStmt.setInt(1, mfhdRs.getInt(1));
			deleteItemStmt.addBatch();
		}
		mfhdRs.close();
		origMfhdStmt.close();
		deleteItemStmt.executeBatch();
		deleteItemStmt.close();

		PreparedStatement deleteMfhdStmt = conn.prepareStatement(
				"DELETE FROM "+CurrentDBTable.MFHD_SOLR.toString()+" WHERE bib_id = ?");
		deleteMfhdStmt.setInt(1, bibid);
		deleteMfhdStmt.executeUpdate();
		deleteMfhdStmt.close();

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
