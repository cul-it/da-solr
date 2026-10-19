package edu.cornell.library.integration.authority.mads;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
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

	@BeforeAll
	public static void setup_test() throws Exception {
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
		// Insertion will happen during the test itself
		// Path extraInit = Path.of(base, "authority_extra_insert.sql");
		// extraInsertSqls = Files.readAllLines(extraInit);
		custom_setup();

		// DbUtils.fetcher = new IFetcher.FileFetcher(rootPath);
		// Use TestFetcher while creating new tests to download missing files from LC
		FileFetcher fileFetcher = new IFetcher.FileFetcher(rootPath);
		HttpFetcher httpFetcher = new IFetcher.HttpFetcher();
		DbUtils.fetcher = new IFetcher.TestFetcher(httpFetcher, fileFetcher);
	}

	@Test
	public void testIndexMadsAuthorityRecords() throws Exception {
		String base = rootPath.toString();
		Path extraInit = Path.of(base, "authority_extra_insert.sql");
		List<String> sqls = Files.readAllLines(extraInit);
		/*
			Index two records - http://id.loc.gov/authorities/genreForms/gf2010025067, http://id.loc.gov/authorities/names/n50000589
			Remove all other records from the update table
		*/
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			List<String> partial = Arrays.asList(sqls.get(0), sqls.get(9));
			DbBaseTest.executeStmt(partial, conn);
		}
		IndexMadsAuthorityRecords.indexAllMadsAuthorityRecords(config, false);

		long cursorAfterIndexAll;
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing all: " + DbUtils.cursor(conn));
			assert DbUtils.cursor(conn) > 0;
			cursorAfterIndexAll = DbUtils.cursor(conn);
			/*
				gf2010025067 is the first row and n50000589 is the 2nd row.
			 */
			assert cursorAfterIndexAll == 2;
			// Verify that the first row corresponds to gf2010025067 and the second row corresponds to n50000589
			try (var ps = conn.prepareStatement("SELECT * FROM %s ORDER BY id ASC".formatted(DbUtils.MADS_AUTHORITY_TABLE))) {
				var rs = ps.executeQuery();
				assert rs.next(); // gf2010025067
				printResultSet(rs);
				assert rs.next(); // n50000589
				printResultSet(rs);
			}
			try (var ps = conn.prepareStatement("SELECT * FROM %s ORDER BY id ASC".formatted(DbUtils.MADS_HEADING_TABLE))) {
				var rs = ps.executeQuery();
				assert rs.next(); // Gravity anamoly maps
				assert rs.next(); // Gravity maps
				assert rs.next(); // Maps
				assert rs.next(); // University of Vermont
			}
		}

		/*
			Index one new record - http://id.loc.gov/authorities/subjects/sh97007140
			Add last element from the extraInsertSqls
		 */
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			DbBaseTest.executeStmt(Arrays.asList(sqls.get(sqls.size() - 1)), conn);
		}
		IndexMadsAuthorityRecords.indexNewMadsAuthorityRecords(config);

		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing new: " + DbUtils.cursor(conn));
			assert DbUtils.cursor(conn) > cursorAfterIndexAll;
			/*
				After indexing the new record, the cursor should reflect the updated ingestId, which is 3.
			 */
			assert DbUtils.cursor(conn) == 3;
			// Verify that the new record corresponds to sh97007140
			try (var ps = conn.prepareStatement("SELECT * FROM %s ORDER BY id ASC".formatted(DbUtils.MADS_HEADING_TABLE))) {
				var rs = ps.executeQuery();
				assert rs.next();
				assert rs.getString("heading").equals("Gravity anomaly maps");
				assert rs.next();
				assert rs.getString("heading").equals("Gravity maps");
				assert rs.next();
				assert rs.getString("heading").equals("Maps");
				assert rs.next();
				assert rs.getString("heading").equals("University of Vermont");
				assert rs.next();
				assert rs.getString("heading").equals("Judeo-Tat language");
			}
		}

		/*
			Update record - http://id.loc.gov/authorities/genreForms/gf2010025067
		 */
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			String updated = sqls.get(0).replace("Gravity anomaly maps", "Gravity anomaly maps X");
			DbBaseTest.executeStmt(Arrays.asList(updated), conn);
		}
		IndexMadsAuthorityRecords.indexNewMadsAuthorityRecords(config);
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing new: " + DbUtils.cursor(conn));
			assert DbUtils.cursor(conn) > cursorAfterIndexAll;
			/*
				After indexing the updated record, the cursor should reflect the updated ingestId, which is 4.
				Gravity anomaly maps should be removed from authority table and Gravity anomaly maps X should be added.
				The heading table should still contain the original "Gravity anomaly maps" entry.
			 */
			assert DbUtils.cursor(conn) == 4;
			System.out.println("Cursor: " + DbUtils.cursor(conn));
			// Verify that the new record corresponds to sh97007140
			try (var ps = conn.prepareStatement("SELECT * FROM %s ORDER BY id ASC".formatted(DbUtils.MADS_AUTHORITY_TABLE))) {
				var rs = ps.executeQuery();
				assert rs.next();
				assert rs.getString("nativeHeading").equals("University of Vermont");
				assert rs.next();
				assert rs.getString("nativeHeading").equals("Judeo-Tat language");
				assert rs.next();
				assert rs.getString("nativeHeading").equals("Gravity anomaly maps X");
				assert !rs.next();
			}
			try (var ps = conn.prepareStatement("SELECT * FROM %s ORDER BY id ASC".formatted(DbUtils.MADS_HEADING_TABLE))) {
				var rs = ps.executeQuery();
				assert rs.next();
				assert rs.getString("heading").equals("Gravity anomaly maps");
				assert rs.next();
				assert rs.getString("heading").equals("Gravity maps");
				assert rs.next();
				assert rs.getString("heading").equals("Maps");
				assert rs.next();
				assert rs.getString("heading").equals("University of Vermont");
				assert rs.next();
				assert rs.getString("heading").equals("Judeo-Tat language");
				assert rs.next();
				assert rs.getString("heading").equals("Bik language");
				assert rs.next();
				assert rs.getString("heading").equals("Dzhuhuric language");
				assert rs.next();
				assert rs.getString("heading").equals("Hebrew Tat language");
				assert rs.next();
				assert rs.getString("heading").equals("Hebrew Tati language");
				assert rs.next();
				assert rs.getString("heading").equals("Jew-Tatish language");
				assert rs.next();
				assert rs.getString("heading").equals("Jewish Tat language");
				assert rs.next();
				assert rs.getString("heading").equals("Judeo-Tatic language");
				assert rs.next();
				assert rs.getString("heading").equals("Juhur language");
				assert rs.next();
				assert rs.getString("heading").equals("Juhuri language");
				assert rs.next();
				assert rs.getString("heading").equals("Juwri language");
				assert rs.next();
				assert rs.getString("heading").equals("Lahji language");
				assert rs.next();
				assert rs.getString("heading").equals("Mountain Jewish language");
				assert rs.next();
				assert rs.getString("heading").equals("Tat language");
				assert rs.next();
				assert rs.getString("heading").equals("Russia (Federation) > Languages");
				assert rs.next();
				assert rs.getString("heading").equals("Gravity anomaly maps X");
				assert !rs.next();
			}
		}
	}

    public static void printResultSet(ResultSet rs) throws SQLException {
        // 1. Get metadata to discover the structure dynamically
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
			String columnName = rsmd.getColumnName(i);
			// getObject handles any data type safely
			Object columnValue = rs.getObject(i); 

			System.out.print(columnName + ": " + columnValue);
			
			// Add a separator between columns
			if (i < columnCount) {
				System.out.print(" | ");
			}
		}
		// Newline after each row
		System.out.println(); 
    }
}
