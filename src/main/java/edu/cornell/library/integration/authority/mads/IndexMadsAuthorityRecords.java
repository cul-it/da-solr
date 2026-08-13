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

import edu.cornell.library.integration.authority.mads.MadsAuthority.MadsHeading;
import edu.cornell.library.integration.authority.mads.MadsAuthority.MadsRelationship;
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

			if (setupDb)
				AuthorityDbUtils.setUpDatabase(headings);

			authority.setAutoCommit(false);
			String maxAddedDate = AuthorityDbUtils.maxAddedDate(authority);
			Set<String> identifiers = AuthorityDbUtils.identifiers(authority);
			authority.setAutoCommit(true);

			headings.setAutoCommit(false);
			for (String identifier : identifiers) {
				processIdentifier(authority, headings, identifier);
				headings.commit();
			}

			AuthorityDbUtils.cursorReplace(headings, maxAddedDate);
			headings.commit();

			return maxAddedDate;
		}
	}

	protected static String indexNewMadsAuthorityRecords(Config config) throws IOException, SQLException, JsonLdError, URISyntaxException, InterruptedException {
		config.setDatabasePoolsize("Headings", 2);
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {
			String cursor = AuthorityDbUtils.cursor(headings);

			authority.setAutoCommit(false);
			String maxAddedDate = AuthorityDbUtils.maxAddedDate(authority);
			Set<String> identifiers = AuthorityDbUtils.identifiersNewerThan(authority, cursor);
			authority.setAutoCommit(true);

			headings.setAutoCommit(false);
			for (String identifier : identifiers) {
				processIdentifier(authority, headings, identifier);
				headings.commit();
			}

			AuthorityDbUtils.cursorReplace(headings, maxAddedDate);
			headings.commit();

			return maxAddedDate;
		}
	}

	protected static void processIdentifier(Connection authority, Connection headings, String identifier)
			throws SQLException, IOException, JsonLdError, URISyntaxException, InterruptedException {
		AuthorityDataMadsSimple latest = AuthorityDbUtils.authorityRecordMostRecent(authority, identifier);
		if (latest == null) return;

		if (latest.lccn == null) latest.lccn = "local " + latest.catalogId();

		removeExistingAuthorityRecord(headings, latest.lccn, latest.id);
		if (MadsRecordStatus.DEPRECATED == latest.recordStatus) {
			System.out.format("%s deprecated; removed existing mapped heading data.\n", identifier);
			return;
		}

		MadsAuthority map = MadsAuthority.fromMadsJsonld(authority, latest.id, latest.source, latest.undifferentiated);
		if (map == null || map.mainHead() == null || map.mainHead().headingType() == null) {
			System.out.format("Skipping %s, unable to derive a main heading.\n", identifier);
			return;
		}

		persistAuthorityMap(headings, map, latest);
	}

	protected static void persistAuthorityMap(Connection headings, MadsAuthority map, AuthorityDataMadsSimple latest)
			throws SQLException, URISyntaxException {
		// map and map.mainHeading() are guaranteed to be not null when called from processIdentifier
		Integer mainHeadingId = getOrCreateHeadingId(headings, map.mainHead());
		Integer authorityId = getOrCreateAuthorityId(headings, latest, mainHeadingId);

		if (authorityId == null) {
			System.out.format("Skipping %s, authority row could not be persisted.\n", latest.id);
			return;
		}

		if (map.notes() != null) {
			for (String note : map.notes()) {
				if (note == null || note.isBlank()) continue;

				AuthorityDbUtils.noteAdd(headings, mainHeadingId, authorityId, note);
			}
		}

		for (MadsRelationship r : map.sees()) {
			if (! r.display()) continue;
			int headingId = getOrCreateHeadingId(headings, r.heading());
			referenceAdd(headings, headingId, mainHeadingId, authorityId, ReferenceType.FROM4XX, r.relationship());
		}

		for (MadsRelationship r : map.seeAlsos()) {
			if (! r.display()) continue;
			int headingId = getOrCreateHeadingId(headings, r.heading());
			referenceAdd(headings, headingId, mainHeadingId, authorityId, ReferenceType.FROM5XX, r.relationship());
			if (r.reciprocalRelationship() != null)
				referenceAdd(headings, mainHeadingId, headingId, authorityId, ReferenceType.TO5XX, r.reciprocalRelationship());
		}

		if (map.rda() != null) AuthorityDbUtils.rdaAdd(headings, mainHeadingId, authorityId, map.rda());
	}

	public static void referenceAdd(Connection headings, int fromId, int toId, int authorityId,
			ReferenceType rt, String relationshipDescription) throws SQLException {
		String description = (relationshipDescription == null) ? "" : relationshipDescription;
		Integer referenceId = AuthorityDbUtils.referenceId(headings, fromId, toId, rt, description);

		if (referenceId == null) referenceId = AuthorityDbUtils.referenceAdd(headings, fromId, toId, rt, description);

		if (referenceId == null) return;

		AuthorityDbUtils.authority2ReferenceReplace(headings, referenceId, authorityId);
	}

	protected static Integer getAuthorityId(Connection headings, AuthorityDataMadsSimple latest) throws SQLException {
		if (latest.lccn == null) return null;

		AuthoritySource source = parseSource(latest.lccn);
		if (source == null) {
			System.out.format("Skipping %s, unable to identify authority source.\n", latest.id);
			return null;
		}

		return AuthorityDbUtils.authorityId(headings, latest.lccn, source);
	}

	protected static Integer getOrCreateAuthorityId(Connection headings, AuthorityDataMadsSimple latest, Integer mainHeadingId)
			throws SQLException, URISyntaxException {
		Integer authorityId = getAuthorityId(headings, latest);
		if ( authorityId != null )
			System.out.println("Possible duplicate authority ID: "+authorityId);
		else
			authorityId = AuthorityDbUtils.authorityAdd(headings, latest);

		if (authorityId == null) return null;

		AuthorityDbUtils.authority2HeadingReplace(headings, mainHeadingId, authorityId);

		return authorityId;
	}

	protected static Integer getOrCreateHeadingId(Connection headings, MadsHeading h) throws SQLException {
		if (h.parent() != null) getOrCreateHeadingId(headings, h.parent());

		MadsHeadingType headingType = h.headingType();

		Integer headingId = AuthorityDbUtils.headingId(headings, headingType, h.sort());
		if (headingId != null) return h.setHeadingId(headingId);

		return AuthorityDbUtils.headingAdd(headings, h);
	}

	protected static void removeExistingAuthorityRecord(Connection headings, String lccn, String id)
			throws SQLException, JsonProcessingException {
		AuthoritySource source = parseSource(lccn);

		Integer authorityId = AuthorityDbUtils.authorityId(headings, lccn, source);
		if (authorityId == null) return; // a new record, nothing to remove

		removeReference(headings, authorityId);

		try (PreparedStatement removeFromAuthority2Heading = AuthorityDbUtils.authority2HeadingRemove(headings);
			 PreparedStatement removeFromAuthority2Reference = AuthorityDbUtils.authority2ReferenceRemove(headings);
			 PreparedStatement removeFromNote = AuthorityDbUtils.noteRemove(headings);
			 PreparedStatement removeFromRda = AuthorityDbUtils.rdaRemove(headings);
			 PreparedStatement removeFromAuthority = AuthorityDbUtils.authorityRemove(headings)) {

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
		try (PreparedStatement checkReferenceWithAuthId = AuthorityDbUtils.checkReferenceWithAuthId(headings);
			 PreparedStatement checkReferenceWithRefId = AuthorityDbUtils.checkReferenceWithRefId(headings);
			 PreparedStatement removeFromAuthority2Reference = AuthorityDbUtils.authority2ReferenceRemove(headings);
			 PreparedStatement removeReference = AuthorityDbUtils.referenceRemove(headings)) {
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

	protected static void addReference(Connection headings, int fromId, int toId, int authorityId,
			ReferenceType rt, String relationshipDescription) throws SQLException {
		String description = (relationshipDescription == null) ? "" : relationshipDescription;
		Integer referenceId = AuthorityDbUtils.referenceId(headings, fromId, toId, rt, description);

		if (referenceId == null) referenceId = AuthorityDbUtils.referenceAdd(headings, fromId, toId, rt, description);

		if (referenceId == null) return;

		AuthorityDbUtils.authority2ReferenceReplace(headings, referenceId, authorityId);
	}

	protected static AuthoritySource parseSource(String lccn) {
		for (AuthoritySource source : AuthoritySource.values())
			if (source.prefix() != null && lccn.startsWith(source.prefix())) return source;
		return null;
	}
}
