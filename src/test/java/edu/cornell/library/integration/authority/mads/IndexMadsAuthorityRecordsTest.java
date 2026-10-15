package edu.cornell.library.integration.authority.mads;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.cornell.library.integration.authority.activitystreams.IFetcher;
import edu.cornell.library.integration.authority.activitystreams.IFetcher.FileFetcher;
import edu.cornell.library.integration.authority.activitystreams.IFetcher.HttpFetcher;
import edu.cornell.library.integration.authority.activitystreams.Utils;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;
import edu.cornell.library.integration.db_test.DbBaseTest;

public class IndexMadsAuthorityRecordsTest extends CustomDbSetupTest {
	static final Path rootPath = Path.of("src", "test", "resources", "edu", "cornell", "library", "integration", "authority");

	/*
		Test data
		gf2010025067 - sees: , seeAlsos: Narrower, Broader
		n00000001 - rda occupation
		n00000203 - rda birthPlace, occupation with rdf:label Author, affiliation, field
		n00000264 - rda in rwo
		n00002040 - notes: 663 (most common)
		n00014709 - has a variant that is also an earlier form that we don't display
		n50071532 - notes: 665
		n78015928 - 500 $w f - Based On
		sh2001001501 - parent heading
		sh2010004770 - 
	 */

	@BeforeEach
	public void setup_test() throws Exception {
		String base = rootPath.toString();
		sqliteExtraSql = Path.of(base, "authority_extra_sqlite_create.sql").toString();
		clearFunctions.add(conn -> {
			try {
				Utils.clearDatabase(conn);
				DbUtils.clearAuthorityDatabase(conn);
				DbUtils.clearHeadingsDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		testContainersSetupFunctions.add(conn -> {
			try {
				Utils.setUpDatabase(conn);
				DbUtils.setUpHeadingsDatabase(conn);
				DbUtils.setUpAuthorityDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		Path extraInit = Path.of(base, "authority_extra_insert.sql");
		extraInsertSqls = Files.readAllLines(extraInit);
		custom_setup();

		// DbUtils.fetcher = new IFetcher.FileFetcher(rootPath);
		FileFetcher fileFetcher;
            fileFetcher = new IFetcher.FileFetcher(rootPath);
		HttpFetcher httpFetcher = new IFetcher.HttpFetcher();
		DbUtils.fetcher = new IFetcher.TestFetcher(httpFetcher, fileFetcher);
	}

	@Test
	public void testIndexMadsAuthorityRecords() throws Exception {
		/*
			Index two records - http://id.loc.gov/authorities/genreForms/gf2010025067, http://id.loc.gov/authorities/names/n50000589
			Remove all other records from the update table
		*/
		List<String> ids = Arrays.asList("http://id.loc.gov/authorities/genreForms/gf2010025067", "http://id.loc.gov/authorities/names/n50000589");
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			try (PreparedStatement pstmt = conn.prepareStatement("SELECT id, ingestId FROM " + DbUtils.MADS_UPDATE_TABLE)) {
				try (var rs = pstmt.executeQuery()) {
					while (rs.next()) {
						System.out.println("ID: " + rs.getString("id") + ", Ingest ID: " + rs.getString("ingestId"));
					}
				}
			}
			String sql = "DELETE FROM " + DbUtils.MADS_UPDATE_TABLE + " WHERE id not in ('" + String.join("','", ids) + "')";
			conn.createStatement().executeUpdate(sql);
		}
		IndexMadsAuthorityRecords.indexAllMadsAuthorityRecords(config, false);

		long cursorAfterIndexAll;
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing all: " + DbUtils.cursor(conn));
			assert DbUtils.cursor(conn) > 0;
			cursorAfterIndexAll = DbUtils.cursor(conn);
			/*
				gf2010025067 is the first row and n50000589 is the 10th row.
				In this test, we removed all other rows and expect the cursor to reflect only the remaining rows.
			 */
			assert cursorAfterIndexAll == 10;
		}

		/*
			Index one new record - http://id.loc.gov/authorities/subjects/sh97007140
			Add last element from the extraInsertSqls
		 */
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			DbBaseTest.executeStmt(Arrays.asList(extraInsertSqls.get(extraInsertSqls.size() - 1)), conn);
		}
		IndexMadsAuthorityRecords.indexNewMadsAuthorityRecords(config);

		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing new: " + DbUtils.cursor(conn));
			assert DbUtils.cursor(conn) > cursorAfterIndexAll;
			/*
				Before we deleted all the rows, there were 34 rows in the table.
				We added the last row again which increases the DbUtils.MADS_UPDATE_TABLE.ingestId by one - 35.
				After indexing the new record, the cursor should reflect the updated ingestId, which is 35.
			 */
			assert DbUtils.cursor(conn) == 35;
		}
	}
}
