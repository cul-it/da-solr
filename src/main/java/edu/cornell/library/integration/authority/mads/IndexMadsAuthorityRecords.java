package edu.cornell.library.integration.authority.mads;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.apicatalog.jsonld.JsonLdError;
import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource;
import edu.cornell.library.integration.metadata.support.AuthorityData.ReferenceType;
import edu.cornell.library.integration.utilities.Config;

public class IndexMadsAuthorityRecords {
	protected static final String ARG_INDEX_ALL = "--index-all";
	protected static final String ARG_SETUP_DB = "--setup-db";

	public static void main(String[] args)
			throws FileNotFoundException, IOException, SQLException, JsonLdError, URISyntaxException, InterruptedException {
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Authority"));
		Config config = Config.loadConfig(requiredArgs);

		if (args.length > 0 && args[0].equalsIgnoreCase(ARG_INDEX_ALL)) {
			boolean setupDb = args.length > 1 && args[1].equalsIgnoreCase(ARG_SETUP_DB);
			indexAllMadsAuthorityRecords(config, setupDb);
		} else {
			indexNewMadsAuthorityRecords(config);
		}
	}

	protected static String indexAllMadsAuthorityRecords(Config config, boolean setupDb) throws IOException, SQLException, JsonLdError, URISyntaxException, InterruptedException {
		config.setDatabasePoolsize("Headings", 2);
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {

			if (setupDb) {
				DbUtils.setUpHeadingsDatabase(headings);
				DbUtils.setupAuthorityDatabase(authority);
			}

			authority.setAutoCommit(false);
			String maxAddedDate = DbUtils.maxAddedDate(authority);
			Set<String> identifiers = DbUtils.identifiers(authority);
			authority.setAutoCommit(true);

			headings.setAutoCommit(false);
			for (String identifier : identifiers) {
				processIdentifier(authority, headings, identifier);
				headings.commit();
			}

			DbUtils.cursorReplace(headings, maxAddedDate);
			headings.commit();

			return maxAddedDate;
		}
	}

	protected static String indexNewMadsAuthorityRecords(Config config) throws IOException, SQLException, JsonLdError, URISyntaxException, InterruptedException {
		config.setDatabasePoolsize("Headings", 2);
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {
			String cursor = DbUtils.cursor(headings);

			authority.setAutoCommit(false);
			String maxAddedDate = DbUtils.maxAddedDate(authority);
			Set<String> identifiers = DbUtils.identifiersNewerThan(authority, cursor);
			authority.setAutoCommit(true);

			headings.setAutoCommit(false);
			for (String identifier : identifiers) {
				if (processIdentifier(authority, headings, identifier))
					headings.commit();
				else
					headings.rollback();
			}

			DbUtils.cursorReplace(headings, maxAddedDate);
			headings.commit();

			return maxAddedDate;
		}
	}

	protected static boolean processIdentifier(Connection authority, Connection headings, String identifier)
			throws SQLException, IOException, JsonLdError, URISyntaxException, InterruptedException {
		AuthorityData latest = DbUtils.authorityRecord(authority, identifier);
		if (latest == null) return false;

		if (latest.lccn == null) latest.lccn = "local " + latest.catalogId();

		removeExistingAuthorityRecord(headings, latest.lccn, latest.id);
		if (RecordStatus.DEPRECATED == latest.recordStatus) {
			System.out.format("%s deprecated; removed existing heading data.\n", identifier);
			return true;
		}

		return indexMadsAuthorityData(authority, headings, latest);
	}

	protected static Heading mainHeading(Connection authority, AuthorityData latest) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		String authLabel = JsonldUtils.getAuthorativeLabel(latest.mainEntry);
		HeadingType t = JsonldUtils.headingType(latest.mainEntry, latest.id);
		return Heading.processHeading(authority, latest.mainEntry, latest.graph, authLabel, null, t);
	}

	protected static boolean indexMadsAuthorityData(Connection authority, Connection headings, AuthorityData latest)
			throws SQLException, URISyntaxException, JsonLdError, IOException, InterruptedException {
		if (latest.lccn != null && latest.lccn.startsWith("sj")) {
			System.out.println("Skipping Juvenile subject authority heading: " + latest.id);
			return false;
		}

		if (latest.isSubdivision) {
			System.out.println("Skipping subdivision authority heading: " + latest.id);
			return false;
		}

		Heading mainHeading = mainHeading(authority, latest);
		if (mainHeading == null || mainHeading.headingType() == null) {
			System.out.format("Skipping %s, unable to derive a main heading.\n", latest.id);
			return false;
		}

		Integer mainHeadingId = getOrCreateHeadingId(headings, mainHeading);
		Integer authorityId = getOrCreateAuthorityId(headings, latest, mainHeadingId);

		if (authorityId == null) {
			System.out.format("Skipping %s, authority row could not be persisted.\n", latest.id);
			return false;
		}

		for (String note : Notes.notes(authority, latest.mainEntry, latest.graph)) {
			if (note == null || note.isBlank()) continue;
			DbUtils.noteAdd(headings, mainHeadingId, authorityId, note);
		}

		for (var r : References.sees(authority, latest.mainEntry, latest.graph, latest.id, mainHeading)) {
			if (! r.display()) continue;

			int headingId = getOrCreateHeadingId(headings, r.heading());
			referenceAdd(headings, headingId, mainHeadingId, authorityId, ReferenceType.FROM4XX, r.relationship());
		}

		for (var r : References.seeAlsos(authority, latest.mainEntry, latest.graph, latest.id, mainHeading)) {
			if (! r.display()) continue;

			int headingId = getOrCreateHeadingId(headings, r.heading());
			referenceAdd(headings, headingId, mainHeadingId, authorityId, ReferenceType.FROM5XX, r.relationship());
			if (r.reciprocalRelationship() != null)
				referenceAdd(headings, mainHeadingId, headingId, authorityId, ReferenceType.TO5XX, r.reciprocalRelationship());
		}

		var rda = Rda.json(Rda.rda(authority, latest));
		if (rda != null) DbUtils.rdaAdd(headings, mainHeadingId, authorityId, rda);

		return true;
	}

	public static void referenceAdd(Connection headings, int fromId, int toId, int authorityId,
			ReferenceType rt, String relationshipDescription) throws SQLException {
		String description = (relationshipDescription == null) ? "" : relationshipDescription;
		Integer referenceId = DbUtils.referenceId(headings, fromId, toId, rt, description);

		if (referenceId == null) referenceId = DbUtils.referenceAdd(headings, fromId, toId, rt, description);

		if (referenceId == null) return;

		DbUtils.authority2ReferenceReplace(headings, referenceId, authorityId);
	}

	protected static Integer getAuthorityId(Connection headings, AuthorityData latest) throws SQLException {
		if (latest.lccn == null) return null;

		AuthoritySource source = parseSource(latest.lccn);
		if (source == null) {
			System.out.format("Skipping %s, unable to identify authority source.\n", latest.id);
			return null;
		}

		return DbUtils.authorityId(headings, latest.lccn, source);
	}

	protected static Integer getOrCreateAuthorityId(Connection headings, AuthorityData latest, Integer mainHeadingId)
			throws SQLException, URISyntaxException {
		Integer authorityId = getAuthorityId(headings, latest);
		if ( authorityId != null )
			System.out.println("Possible duplicate authority ID: "+authorityId);
		else
			authorityId = DbUtils.authorityAdd(headings, latest);

		if (authorityId == null) return null;

		DbUtils.authority2HeadingReplace(headings, mainHeadingId, authorityId);

		return authorityId;
	}

	protected static Integer getOrCreateHeadingId(Connection headings, Heading h) throws SQLException {
		if (h.parent() != null) getOrCreateHeadingId(headings, h.parent());

		HeadingType headingType = h.headingType();

		Integer headingId = DbUtils.headingId(headings, headingType, h.sort());
		if (headingId != null) return h.setHeadingId(headingId);

		return DbUtils.addHeading(headings, h);
	}

	protected static void removeExistingAuthorityRecord(Connection headings, String lccn, String id)
			throws SQLException, JsonProcessingException {
		AuthoritySource source = parseSource(lccn);

		Integer authorityId = DbUtils.authorityId(headings, lccn, source);
		if (authorityId == null) return; // a new record, nothing to remove

		removeReference(headings, authorityId);

		try (PreparedStatement removeFromAuthority2Heading = DbUtils.authority2HeadingRemove(headings);
			 PreparedStatement removeFromAuthority2Reference = DbUtils.authority2ReferenceRemove(headings);
			 PreparedStatement removeFromNote = DbUtils.noteRemove(headings);
			 PreparedStatement removeFromRda = DbUtils.rdaRemove(headings);
			 PreparedStatement removeFromAuthority = DbUtils.authorityRemove(headings)) {

			for (PreparedStatement pstmt : Arrays.asList(
					removeFromAuthority2Heading,
					removeFromAuthority2Reference,
					removeFromNote,
					removeFromRda,
					removeFromAuthority)) {
				pstmt.setInt(1, authorityId);
				pstmt.executeUpdate();
			}
		}
	}

	protected static void removeReference(Connection headings, int authorityId) throws SQLException {
		try (PreparedStatement checkReferenceWithAuthId = DbUtils.checkReferenceWithAuthId(headings);
			 PreparedStatement checkReferenceWithRefId = DbUtils.checkReferenceWithRefId(headings);
			 PreparedStatement removeFromAuthority2Reference = DbUtils.authority2ReferenceRemove(headings);
			 PreparedStatement removeReference = DbUtils.referenceRemove(headings)) {
			checkReferenceWithAuthId.setInt(1, authorityId);
			List<Integer> refIds = new java.util.ArrayList<>();
			try (ResultSet rs = checkReferenceWithAuthId.executeQuery()) {
				while (rs.next()) refIds.add(rs.getInt(1));
			}
			removeFromAuthority2Reference.setInt(1, authorityId);
			removeFromAuthority2Reference.executeUpdate();
			for (Integer refId : refIds) {
				checkReferenceWithRefId.setInt(1, refId);
				try (ResultSet refRs = checkReferenceWithRefId.executeQuery()) {
					if (refRs.next()) continue;

					removeReference.setInt(1, refId);
					removeReference.executeUpdate();
				}
			}
		}
	}

	// protected static void addReference(Connection headings, int fromId, int toId, int authorityId,
	// 		ReferenceType rt, String relationshipDescription) throws SQLException {
	// 	String description = (relationshipDescription == null) ? "" : relationshipDescription;
	// 	Integer referenceId = AuthorityDbUtils.referenceId(headings, fromId, toId, rt, description);

	// 	if (referenceId == null) referenceId = AuthorityDbUtils.referenceAdd(headings, fromId, toId, rt, description);

	// 	if (referenceId == null) return;

	// 	AuthorityDbUtils.authority2ReferenceReplace(headings, referenceId, authorityId);
	// }

	protected static AuthoritySource parseSource(String lccn) {
		for (AuthoritySource source : AuthoritySource.values())
			if (source.prefix() != null && lccn.startsWith(source.prefix())) return source;
		return null;
	}
}
