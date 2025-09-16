package edu.cornell.library.integration.authority;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import edu.cornell.library.integration.authority.IndexAuthorityRecords.AuthorityData;
import edu.cornell.library.integration.db_test.DbBaseTest;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class IndexAuthorityRecordsTest extends DbBaseTest {
	protected List<Integer> authorityIdToClean = new ArrayList<>();
	protected Map<String, String> cleanUpDefinition = Map.of(
			"authority2heading", "authority_id",
			"authority2reference", "authority_id",
			"note", "authority_id",
			"rda", "authority_id",
			"authority", "id"
			);

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
			IndexAuthorityRecords.indexAllAuthorityRecords(false);

			PreparedStatement authByNativeId = headings.prepareStatement("SELECT id FROM authority WHERE source = 1 AND nativeId = ?");
			Integer authId = dbQueryGetInt(authByNativeId, "sh 85066169");
			assert authId != null;

			PreparedStatement headingRel = headings.prepareStatement("SELECT heading_id FROM authority2heading WHERE authority_id = ?");
			List<Integer> headingIds = dbQuery(headingRel, authId);
			assert headingIds.size() == 2;

			PreparedStatement referenceRel = headings.prepareStatement("SELECT * FROM authority2reference WHERE authority_id = ?");
			Integer referenceId = dbQueryGetInt(referenceRel, authId);
			assert referenceId != null;

			String cursor = IndexAuthorityRecords.getCursor(headings);
			LocalDate today = LocalDate.now();
			assert cursor.equalsIgnoreCase(today.toString());
		}
	}

	@Test
	@Order(2)
	public void testIndexNewAuthorityRecords() throws SQLException, FileNotFoundException, IOException {
		try ( Connection authority = config.getDatabaseConnection("Authority");
			  Connection headings = config.getDatabaseConnection("Headings") ) {
			Set<MarcRecord> recs = IndexAuthorityRecords.getNewMarcRecords(authority, "2025-01-15");
			assert recs.size() == 0;

			recs = IndexAuthorityRecords.getNewMarcRecords(authority, "2021-01-15");
			assert recs.size() == 1;

			Map<String, Integer> authIds = new TreeMap<>();
			for (MarcRecord mr : recs) {
				AuthorityData ad = IndexAuthorityRecords.parseMarcRecord(mr);
				authIds.put(ad.lccn, ad.id);
			}

			/*
			 * To test the heading logic, we will manually update one of the heading record.
			 */
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

			IndexAuthorityRecords.updateCursor(headings, "2021-01-15");
			IndexAuthorityRecords.indexNewAuthorityRecords();

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

			String cursor = IndexAuthorityRecords.getCursor(headings);
			LocalDate today = LocalDate.now();
			assert cursor.equalsIgnoreCase(today.toString());
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
