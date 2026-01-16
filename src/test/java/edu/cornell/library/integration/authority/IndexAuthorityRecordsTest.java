package edu.cornell.library.integration.authority;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexAuthorityRecordsTest extends DbBaseTest {
	// This value is the max year week portion of authorityFile column from the test data we added to authorityUpdate table.
	static final String MAX_YEAR_WEEK = "25.01";
	static final String STARTING_CURSOR = "20.01";
	static final String IGNORED_UPDATE_FILE = "unname99.01";
	static final String NEW_UPDATE_FILE = "unname25.02";
	static final String NEW_MAX_YEAR_WEEK = "25.02";

	@BeforeClass
	public static void setup() throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Headings");
		requiredArgs.addAll( Config.getRequiredArgsForDB("Authority"));
		setup("Headings", requiredArgs);
		try ( Connection headings = config.getDatabaseConnection("Headings")) {
			IndexAuthorityRecords.populateStaticData(headings);
		}
	}

	@Test
	@Order(1)
	public void testIndexAllAuthorityRecords() throws SQLException, FileNotFoundException, IOException {
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {
			String cursor = IndexAuthorityRecords.indexAllAuthorityRecords(config, false);

			PreparedStatement authByNativeId = headings.prepareStatement("SELECT id FROM authority WHERE source = 1 AND nativeId = ?");
			Integer authId = dbQueryGetInt(authByNativeId, "sh 85066169");
			assert authId != null;

			PreparedStatement headingRel = headings.prepareStatement("SELECT heading_id FROM authority2heading WHERE authority_id = ?");
			List<Integer> headingIds = dbQuery(headingRel, authId);
			assert headingIds.size() == 2;

			PreparedStatement referenceRel = headings.prepareStatement("SELECT * FROM authority2reference WHERE authority_id = ?");
			Integer referenceId = dbQueryGetInt(referenceRel, authId);
			assert referenceId != null;

			assertEquals(MAX_YEAR_WEEK, cursor, "New cursor should be " + MAX_YEAR_WEEK);
		}
	}

	@Test
	@Order(2)
	public void testIndexNewAuthorityRecords() throws SQLException, FileNotFoundException, IOException {
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {
			Set<String> identifiers = IndexAuthorityRecords.getNewIdentifiers(authority, MAX_YEAR_WEEK);
			assertEquals(0, identifiers.size());

			identifiers = IndexAuthorityRecords.getNewIdentifiers(authority, STARTING_CURSOR);
			assertEquals(2, identifiers.size());

			String deletedIdentifier = "sh 85066170";
			MarcRecord rec = IndexAuthorityRecords.getMostRecentRecord(authority, deletedIdentifier);
			assert rec.leader.startsWith("00352dz");

			// To test the heading logic, we will manually update one of the heading record.
			PreparedStatement authByNativeId = headings.prepareStatement("SELECT id FROM authority WHERE source = 1 AND nativeId = ?");
			PreparedStatement authById = headings.prepareStatement("SELECT id FROM authority WHERE id = ?");
			PreparedStatement heading = headings.prepareStatement("SELECT id FROM heading WHERE id = ?");
			PreparedStatement headingRel = headings.prepareStatement("SELECT heading_id FROM authority2heading WHERE authority_id = ?");
			PreparedStatement reference = headings.prepareStatement("SELECT * FROM reference WHERE id = ?");
			PreparedStatement referenceRel = headings.prepareStatement("SELECT * FROM authority2reference WHERE authority_id = ?");

			Integer existingAuthId = dbQueryGetInt(authByNativeId, "sh 85066169");
			List<Integer> existingHeadingIds = dbQuery(headingRel, existingAuthId);
			Integer existingReferenceId = dbQueryGetInt(referenceRel, existingAuthId);
			PreparedStatement headingUpdate = headings.prepareStatement("UPDATE heading set sort = 'bogus' WHERE id = ?");
			headingUpdate.setInt(1, existingHeadingIds.get(0));
			headingUpdate.executeUpdate();

			/*
			 * The test data contains a record whose updateFile is set to 99.01 so it wouldn't get picked up initially.
			 * Set the updateFile to new max for indexNewAuthorityRecords test.
			 */
			PreparedStatement changeUpdateFile = authority.prepareStatement("UPDATE authorityUpdate SET updateFile = ? WHERE updateFile = ?");
			changeUpdateFile.setString(1, NEW_UPDATE_FILE);
			changeUpdateFile.setString(2, IGNORED_UPDATE_FILE);
			changeUpdateFile.executeUpdate();
			String cursor = IndexAuthorityRecords.indexNewAuthorityRecords(config);
			assertEquals(NEW_MAX_YEAR_WEEK, cursor, "New cursor should match the max year week of updateFile in authorityUpdate");

			Integer newAuthId = dbQueryGetInt(authByNativeId, "sh 85066169");
			assert newAuthId != null;
			assert existingAuthId != newAuthId : "A new authority record should be created.";
			Integer authCheck = dbQueryGetInt(authById, existingAuthId);
			assert authCheck == null;

			List<Integer> newHeadingIds = dbQuery(headingRel, newAuthId);
			assert existingHeadingIds.size() == 2;
			assert newHeadingIds.size() == 2;
			newHeadingIds.removeAll(existingHeadingIds);
			assert newHeadingIds.size() == 1 : "Only one of the heading should be newly created, diff size: " + newHeadingIds.size();
			for (Integer headingId : existingHeadingIds) {
				Integer headingCheck = dbQueryGetInt(heading, headingId);
				assert headingCheck != null : "Existing heading record with id " + headingId + " should not be deleted.";
			}

			Integer newReferenceId = dbQueryGetInt(referenceRel, newAuthId);
			assert existingReferenceId != newReferenceId : "New references should be populated.";
			Integer headingCheck = dbQueryGetInt(reference, existingReferenceId);
			assert headingCheck == null : "Dangling reference record with id " + existingReferenceId + " should be deleted.";
		}
	}

	public Integer dbQueryGetInt(PreparedStatement pstmt, String arg) throws SQLException {
		List<Integer> results = dbQuery(pstmt, arg);
		if (results.size() > 0)
			return results.get(0);
		else
			return null;
	}

	public Integer dbQueryGetInt(PreparedStatement pstmt, int arg) throws SQLException {
		List<Integer> results = dbQuery(pstmt, arg);
		if (results.size() > 0)
			return results.get(0);
		else
			return null;
	}

	public List<Integer> dbQuery(PreparedStatement pstmt, String arg) throws SQLException {
		pstmt.setString(1, arg);
		ResultSet rs = pstmt.executeQuery();
		List<Integer> result = new ArrayList<>();
		while (rs.next()) {
			result.add(rs.getInt(1));
		}

		return result;
	}

	public List<Integer> dbQuery(PreparedStatement pstmt, int arg) throws SQLException {
		pstmt.setInt(1, arg);
		ResultSet rs = pstmt.executeQuery();
		List<Integer> result = new ArrayList<>();
		while (rs.next()) {
			result.add(rs.getInt(1));
		}

		return result;
	}
}
