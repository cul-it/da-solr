package edu.cornell.library.integration.indexer.documentPostProcess;

import static edu.cornell.library.integration.utilities.IndexingUtilities.pullReferenceFields;

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

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.utilities.IndexingUtilities.TitleMatchReference;
import edu.cornell.library.integration.voyager.IdentifyChangedRecords.DataChangeUpdateType;

/** Evaluate populated fields for conditions of membership for any collections.
 * (Currently only "Law Library".)
 *  */
public class UpdateSolrInventoryDB implements DocumentPostProcess{

	private final SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	private Set<Long> workids = new HashSet<>();
	private static ObjectMapper mapper = new ObjectMapper();
	
	@Override
	public void p(String recordURI, Config config,
			SolrInputDocument document) throws Exception {

		try (Connection conn = config.getDatabaseConnection("Current")) {

			String bibid_display = document.getField("bibid_display").getValue().toString();
			String[] tmp = bibid_display.split("\\|",2);
			Integer bibid = Integer.valueOf(tmp[0]);
			if ( config.getTestMode() ) {
				compareDocument(conn,document, bibid);
				return;
			}

		// compare bib, mfhd and item list and dates to inventory, updating if need be
		Timestamp origBibDate = null;
		final String origBibDateQuery =
				"SELECT record_date FROM bibRecsSolr WHERE bib_id = ?";
		try (PreparedStatement origBibDateStmt = conn.prepareStatement(origBibDateQuery)) {
			origBibDateStmt.setInt(1, bibid);
			try (ResultSet rs = origBibDateStmt.executeQuery()) {
				while (rs.next()) {
					origBibDate = rs.getTimestamp(1);
				}
			}
		}
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
			removeOrigHoldingsDataFromDB( conn, bibid );
			knockOnUpdatesNeeded = updateBibField( conn, document, bibid);
			populateHoldingFields( conn, document, bibid );
			populateItemFields( conn, document );
			updateWorkInfo( conn, document, bibid );
		}

		addWorkIdLinksToDocument(conn,document,bibid,knockOnUpdatesNeeded);
		markBibAsDoneInIndexQueue(conn,bibid);
		pushSolrDocumentToDatabase(conn,document,bibid);
		}
	}

	private static void compareDocument(Connection conn, SolrInputDocument document, Integer bibid) throws SQLException {
		final String solrDocumentFromDBQuery = 
				"SELECT solr_document FROM bibRecsSolr WHERE bib_id = ?";
		String origDoc = null;
		try (PreparedStatement solrDocumentFromDB = conn.prepareStatement(solrDocumentFromDBQuery)) {
			solrDocumentFromDB.setInt(1, bibid);
			try (ResultSet rs = solrDocumentFromDB.executeQuery()) {
				while (rs.next())
					origDoc = rs.getString(1);
			}
		}
		if (origDoc != null) {
			String newDoc = ClientUtils.toXML(document).replaceAll("</field>","$0\n");
			System.out.println("*** "+bibid+" ***");
			System.out.println(newDoc);
			//SU.difference isn't working, so I'll just display the new doc until trying a new diff solution.
//			System.out.println(StringUtils.difference(origDoc, newDoc));
		}
	}

	private static void pushSolrDocumentToDatabase(Connection conn, SolrInputDocument document, Integer bibid) throws SQLException {
		final String solrDocumentToDBQuery = 
				"UPDATE bibRecsSolr SET solr_document = ? WHERE bib_id = ?";
		try (PreparedStatement solrDocumentToDB = conn.prepareStatement(solrDocumentToDBQuery)) {
		solrDocumentToDB.setString(1,ClientUtils.toXML(document).replaceAll("</field>","$0\n"));
		solrDocumentToDB.setInt(2,bibid);
		solrDocumentToDB.executeUpdate();
		}
	}

	private static void markBibAsDoneInIndexQueue(Connection conn, Integer bibid) throws SQLException {
		final String markDoneInQueueQuery =
				"UPDATE indexQueue SET done_date = NOW() WHERE bib_id = ? AND done_date = 0";
		try (PreparedStatement markDoneInQueueStmt = conn.prepareStatement(markDoneInQueueQuery)) {
		markDoneInQueueStmt.setInt(1, bibid);
		markDoneInQueueStmt.executeUpdate();
		}
	}

	private void addWorkIdLinksToDocument(Connection conn,
			SolrInputDocument document, int bibid, boolean knockOnUpdatesNeeded) throws Exception {
		if (workids.isEmpty())
			return;
		if (workids.size() > 1)
			System.out.println("bib with multiple associated works: "+bibid);

		final String recordsForWorkQuery =
				"SELECT bib.* FROM bibRecsSolr AS bib, bib2work AS works "
				+ "WHERE works.bib_id = bib.bib_id "
				+ "AND works.work_id = ? "
				+ "AND bib.active = 1 AND works.active = 1 ";
		final String markBibForUpdateQuery =
				"INSERT INTO indexQueue"
				+ " (bib_id, priority, cause) VALUES"
				+ " (?,"+DataChangeUpdateType.TITLELINK.getPriority().ordinal()+", '"
						+DataChangeUpdateType.TITLELINK+"')";
		SolrInputField workidDisplay = new SolrInputField("workid_display");
		SolrInputField workidFacet = new SolrInputField("workid_facet");
		SolrInputField otherAvailPiped = new SolrInputField("other_availability_piped");
		SolrInputField otherAvailJson = new SolrInputField("other_availability_json");
		Map<Integer,TitleMatchReference> refs = new HashMap<>();
		TitleMatchReference thisTitle = null;
		try (   PreparedStatement recordsForWorkStmt = conn.prepareStatement(recordsForWorkQuery);
				PreparedStatement markBibForUpdateStmt = conn.prepareStatement(markBibForUpdateQuery)  ) {

			for (long workid : workids) {
				workidFacet.addValue(workid, 1);
				recordsForWorkStmt.setLong(1, workid);
				int referenceCount = 0;
				try (   ResultSet rs = recordsForWorkStmt.executeQuery()   ) {

					while (rs.next()) {
						int refBibid = rs.getInt("bib_id");
						if (bibid == refBibid) {
							if (thisTitle == null) {
								thisTitle = new TitleMatchReference();
								thisTitle.id = bibid;
								thisTitle.format = rs.getString("format");
								thisTitle.url = rs.getString("url");
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
								ref.url = rs.getString("url");
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
				}
				if (referenceCount >= 1)
					workidDisplay.addValue(String.valueOf(workid)+"|"
							+String.valueOf(referenceCount+1), 1);
			}
			markBibForUpdateStmt.executeBatch();
		}

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
	private static String generateJsonAvailability(TitleMatchReference ref)  throws Exception {
		Map<String,Object> json = new HashMap<>();
		json.put("bibid", ref.id);
		json.put("format", ref.format);
		if (ref.url != null && ! ref.url.isEmpty())
			json.put("url", ref.url);
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

	private static String generatePipedAvailability(TitleMatchReference thisTitle,TitleMatchReference ref) {
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
		List<HoldingRecord> holdings = new ArrayList<>();
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
		List<ItemRecord> items = new ArrayList<>();
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

	private static Set<Integer> extractOclcIdsFromSolrField(Collection<Object> fieldValues) {
		Set<Integer> oclcIds = new HashSet<>();
		if (fieldValues == null) return oclcIds;
		for (Object id_obj : fieldValues) {
			String id = id_obj.toString();
			if (id.startsWith("(OCoLC)")) {
				try {
					oclcIds.add(Integer.valueOf(id.substring(7)));
				} catch (@SuppressWarnings("unused") NumberFormatException e) {
					// Ignore the value if it's invalid
				}
			}
		}
		return oclcIds;
	}

	/* s1.equals(s2) will produce NPE if s1 is null. */
	private static boolean stringsEqual( String s1, String s2 ) {
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

	private static Boolean updateBibField(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		TitleMatchReference ref = pullReferenceFields(document);

		// pull existing values from DB are the descriptive fields changed?
		final String origDescQuery =
				"SELECT format, url, sites, libraries, edition, pub_date, title, language, active"
						+ " FROM bibRecsSolr WHERE bib_id = ?";
		boolean descChanged = false;
		try (  PreparedStatement origDescStmt = conn.prepareStatement( origDescQuery )  ) {

			origDescStmt.setInt(1, bibid);
			try (  ResultSet origDescRS = origDescStmt.executeQuery()  ) {
				while (origDescRS.next())
					if (  ! stringsEqual(ref.format,origDescRS.getString("format"))
							|| ! stringsEqual(ref.url,origDescRS.getString("url"))
							|| ! stringsEqual(ref.sites,origDescRS.getString("sites"))
							|| ! stringsEqual(ref.libraries,origDescRS.getString("libraries"))
							|| ! stringsEqual(ref.edition,origDescRS.getString("edition"))
							|| ! stringsEqual(ref.pub_date,origDescRS.getString("pub_date"))
							|| ! stringsEqual(ref.title,origDescRS.getString("title"))
							|| ! stringsEqual(ref.language,origDescRS.getString("language"))
							|| ! origDescRS.getBoolean("active"))
						descChanged = true;
			}
		}

		// update db appropriately
		if (descChanged) {
			final String updateDescQuery =
					"UPDATE bibRecsSolr"
					+" SET record_date = ?, format = ?, "
					+ "url = ?, sites = ?, libraries = ?, "
					+ "edition = ?, pub_date = ?, title = ?, language = ?, "
					+ "index_date = NOW(), linking_mod_date = NOW(), "
					+ "active = 1 "
					+ "WHERE bib_id = ?";
			try (PreparedStatement updateDescStmt = conn.prepareStatement(updateDescQuery)) {
				updateDescStmt.setTimestamp(1, ref.timestamp);
				updateDescStmt.setString(2, ref.format);
				updateDescStmt.setString(3, ref.url);
				updateDescStmt.setString(4, ref.sites);
				updateDescStmt.setString(5, ref.libraries);
				updateDescStmt.setString(6, ref.edition);
				updateDescStmt.setString(7, ref.pub_date);
				updateDescStmt.setString(8, ref.title);
				updateDescStmt.setString(9, ref.language);
				updateDescStmt.setInt(10, bibid);
				updateDescStmt.executeUpdate();
			}
		}
		if ( ! descChanged ) {
			final String updateIndexedDateQuery =
					"UPDATE bibRecsSolr SET record_date = ?, index_date = NOW(), active = 1 WHERE bib_id = ?";
			try (  PreparedStatement updateIndexedDateStmt = conn.prepareStatement(updateIndexedDateQuery)  ) {
				updateIndexedDateStmt.setTimestamp(1, ref.timestamp);
				updateIndexedDateStmt.setInt(2, bibid);
				updateIndexedDateStmt.executeUpdate();
			}
		}
		
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
			oclcIds = new HashSet<>();
		}
		Set<Integer> previousOclcIds = new HashSet<>();
		final String origWorksQuery =
				"SELECT DISTINCT oclc_id FROM bib2work WHERE bib_id = ? AND active = 1";
		try (  PreparedStatement origWorksStmt = conn.prepareStatement(origWorksQuery)  ){

			origWorksStmt.setInt(1, bibid);
			try (  ResultSet rs = origWorksStmt.executeQuery()  ) {

				while (rs.next()) {
					previousOclcIds.add(rs.getInt(1));
				}
			}
		}

		// new
		Set<Integer> newOclcIds = new HashSet<>(oclcIds);
		newOclcIds.removeAll(previousOclcIds);
		populateWorkInfo(conn, newOclcIds, bibid);

		// removed
		Set<Integer> removedOclcIds = new HashSet<>(previousOclcIds);
		removedOclcIds.removeAll(oclcIds);
		deactivateWorkInfo(conn, removedOclcIds, bibid);

		// retained
		oclcIds.retainAll(previousOclcIds);
		identifyRetainedWorkIds(conn, oclcIds);
	}

	private static void populateBibField(Connection conn, SolrInputDocument document) throws SQLException, ParseException {
		TitleMatchReference ref = pullReferenceFields(document);

		final String insertBibQuery =
				"INSERT INTO bibRecsSolr"+
				" (bib_id, record_date, format, sites, libraries, "
				+ "index_date, edition, pub_date, title, language) "
						+ "VALUES (?, ?, ?, ?, ?, NOW(), ?, ?, ?, ?)";
		try (  PreparedStatement insertBibStmt = conn.prepareStatement(insertBibQuery) ){
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
		}
	}

	private void populateHoldingFields(Connection conn, SolrInputDocument document,
			Integer bibid) throws SQLException, ParseException {

		List<HoldingRecord> holdings = extractHoldingsFromSolrField(
				document.getFieldValues("holdings_display"));
		final String insertMfhdQuery =
				"INSERT INTO mfhdRecsSolr (bib_id, mfhd_id, record_date) VALUES (?, ?, ?)";
		try (  PreparedStatement insertMfhdStmt = conn.prepareStatement(insertMfhdQuery)  ){

			for (HoldingRecord holding : holdings ) {
				insertMfhdStmt.setInt(1, bibid);
				insertMfhdStmt.setInt(2, holding.id);
				insertMfhdStmt.setTimestamp(3, holding.modified);
				insertMfhdStmt.addBatch();
			}
			insertMfhdStmt.executeBatch();
		}
	}

	private void populateItemFields(Connection conn, SolrInputDocument document) throws SQLException, ParseException {

		if ( ! document.containsKey("item_display") )
			return;
		List<ItemRecord> items = extractItemsFromSolrField(
				document.getFieldValues("item_display"));
		final String insertItemQuery =
				"INSERT INTO itemRecsSolr (mfhd_id, item_id, record_date) VALUES (?, ?, ?)";
		try (  PreparedStatement insertItemStmt = conn.prepareStatement(insertItemQuery)  ){

			for (ItemRecord item : items ) {
				insertItemStmt.setInt(1, item.mfhdid);
				insertItemStmt.setInt(2, item.id);
				insertItemStmt.setTimestamp(3, item.modified);
				insertItemStmt.addBatch();
			}
			insertItemStmt.executeBatch();
		}
	}

	private void identifyRetainedWorkIds(Connection conn, Set<Integer> oclcIds) throws SQLException {

		final String findWorksForOclcIdQuery =
			"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?";
		try (  PreparedStatement findWorksForOclcIdStmt = conn.prepareStatement(findWorksForOclcIdQuery)  ){

			for (int oclcId : oclcIds) {
				findWorksForOclcIdStmt.setInt(1, oclcId);
				try ( ResultSet rs = findWorksForOclcIdStmt.executeQuery()  ) {

					while (rs.next())
						workids.add(rs.getLong(1));
				}
			}
		}
	}
	private void populateWorkInfo(Connection conn, Set<Integer> oclcIds, int bibid) throws SQLException {

		final String findWorksForOclcIdQuery =
			"SELECT work_id FROM workids.work2oclc WHERE oclc_id = ?";
		final String checkForDeactivatedBibWorkMappingQuery =
				"SELECT * FROM bib2work WHERE bib_id = ? AND oclc_id = ? AND work_id = ?";
		final String activateBibWorkMappingQuery =
				"UPDATE bib2work"+
				"  SET active = 1, mod_date = NOW() "+
				" WHERE bib_id = ? AND oclc_id = ? AND work_id = ?";
		final String insertBibWorkMappingQuery =
				"INSERT INTO bib2work (bib_id, oclc_id, work_id) VALUES (?, ?, ?)";
		final String findBibsForAddedWorksQuery =
				"SELECT bib_id"
				+ " FROM bib2work"
				+" WHERE work_id = ?"
				+ "  AND oclc_id = ?"
				+ "  AND bib_id != ?"
				+ "  AND active = 1";
		final String markBibForUpdateQuery =
				"INSERT INTO indexQueue (bib_id, priority, cause)"
				+ " VALUES (?,"+DataChangeUpdateType.TITLELINK.getPriority().ordinal()+", '"
						+DataChangeUpdateType.TITLELINK+"')";

		try (   PreparedStatement findWorksForOclcIdStmt = conn.prepareStatement(findWorksForOclcIdQuery);
				PreparedStatement checkForDeactivatedBibWorkMappingStmt =
						conn.prepareStatement(checkForDeactivatedBibWorkMappingQuery);
				PreparedStatement activateBibWorkMappingStmt = conn.prepareStatement(activateBibWorkMappingQuery);
				PreparedStatement insertBibWorkMappingStmt = conn.prepareStatement(insertBibWorkMappingQuery);
				PreparedStatement findBibsForAddedWorksStmt = conn.prepareStatement(findBibsForAddedWorksQuery);
				PreparedStatement markBibForUpdateStmt = conn.prepareStatement(markBibForUpdateQuery)  ) {

		for (int oclcId : oclcIds) {
			findWorksForOclcIdStmt.setInt(1, oclcId);
			try (  ResultSet rs = findWorksForOclcIdStmt.executeQuery()  ){
				while (rs.next()) {
					long workid = rs.getLong(1);
					// Is the mapping already present, but deactivated?
					checkForDeactivatedBibWorkMappingStmt.setInt(1, bibid);
					checkForDeactivatedBibWorkMappingStmt.setInt(2, oclcId);
					checkForDeactivatedBibWorkMappingStmt.setLong(3, workid);
					boolean isDeactivated = false;
					try (  ResultSet isDeactivatedRS = checkForDeactivatedBibWorkMappingStmt.executeQuery()  ){
						while (isDeactivatedRS.next())
							isDeactivated = true;
					}

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
					try (  ResultSet knockOnRS = findBibsForAddedWorksStmt.executeQuery()  ){
						while (knockOnRS.next()) {
							markBibForUpdateStmt.setInt(1, knockOnRS.getInt(1));
							markBibForUpdateStmt.addBatch();
						}
					}
				}
			}
		}
		insertBibWorkMappingStmt.executeBatch();
		activateBibWorkMappingStmt.executeBatch();
		markBibForUpdateStmt.executeBatch();
		}
	}

	private static void deactivateWorkInfo(Connection conn, Set<Integer> removedOclcIds, int bibid) throws SQLException {

		if (removedOclcIds.isEmpty()) 
			return;
		final String updateBibWorkMappingQuery =
				"UPDATE bib2work SET active = 0, mod_date = NOW() WHERE bib_id = ? AND oclc_id = ?";
		final String findBibsForDeactivatedWorksQuery =
				"SELECT distinct b.bib_id"
				+ " FROM bib2work as a, bib2work as b"
				+ " WHERE a.work_id = b.work_id"
				+ "   AND a.bib_id = ? AND a.oclc_id = ?"
				+ "   AND b.bib_id != a.bib_id"
				+ "   AND b.active = 1";
		final String markBibForUpdateQuery =
				"INSERT INTO indexQueue"
				+ " (bib_id, priority, cause) VALUES"
				+ " (?,"+DataChangeUpdateType.TITLELINK.getPriority().ordinal()+", '"
						+DataChangeUpdateType.TITLELINK+"')";
		try (   PreparedStatement updateBibWorkMappingStmt = conn.prepareStatement(updateBibWorkMappingQuery);
				PreparedStatement findBibsForDeactivatedWorksStmt = conn.prepareStatement(findBibsForDeactivatedWorksQuery);
				PreparedStatement markBibForUpdateStmt = conn.prepareStatement(markBibForUpdateQuery)   ){

			for (int oclcid : removedOclcIds) {
				updateBibWorkMappingStmt.setInt(1, bibid);
				updateBibWorkMappingStmt.setInt(2, oclcid);
				updateBibWorkMappingStmt.addBatch();
				findBibsForDeactivatedWorksStmt.setInt(1, bibid);
				findBibsForDeactivatedWorksStmt.setInt(2, oclcid);
				try (  ResultSet rs = findBibsForDeactivatedWorksStmt.executeQuery()  ){
					while (rs.next()) {
						markBibForUpdateStmt.setInt(1, rs.getInt(1));
						markBibForUpdateStmt.addBatch();
					}
				}
			}
			updateBibWorkMappingStmt.executeBatch();
			markBibForUpdateStmt.executeBatch();
		}
	}

	private static List<HoldingRecord> removeOrigHoldingsDataFromDB(
			Connection conn, int bibid) throws SQLException {
		List<HoldingRecord> holdings = new ArrayList<>();
		final String origMfhdQuery =
				"SELECT mfhd_id FROM mfhdRecsSolr WHERE bib_id = ?";
		final String deleteItemQuery =
				"DELETE FROM itemRecsSolr WHERE mfhd_id = ?";
		try (   PreparedStatement origMfhdStmt = conn.prepareStatement(origMfhdQuery);
				PreparedStatement deleteItemStmt = conn.prepareStatement(deleteItemQuery)   ){

			origMfhdStmt.setInt(1, bibid);
			try (  ResultSet mfhdRs = origMfhdStmt.executeQuery()  ){

				while (mfhdRs.next()) {
					deleteItemStmt.setInt(1, mfhdRs.getInt(1));
					deleteItemStmt.addBatch();
				}
			}
			deleteItemStmt.executeBatch();
		}

		try (  PreparedStatement deleteMfhdStmt = conn.prepareStatement(
				"DELETE FROM mfhdRecsSolr WHERE bib_id = ?")  ){

			deleteMfhdStmt.setInt(1, bibid);
			deleteMfhdStmt.executeUpdate();
		}

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
