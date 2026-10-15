package edu.cornell.library.integration.authority.mads;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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

	protected void initUpdateTable(List<String> ids) throws SQLException, UnsupportedEncodingException, FileNotFoundException, IOException {
		try (Connection conn = config.getDatabaseConnection("Headings")) {
			for (String extraSql : extraInsertSqls)
				DbBaseTest.executeStmt(Arrays.asList(extraSql), conn);
			String sql = "DELETE FROM " + DbUtils.MADS_UPDATE_TABLE + " WHERE id not in ('" + String.join("','", ids) + "')";
			conn.createStatement().executeUpdate(sql);
		}
	}

	@Test
	public void testIndexMadsAuthorityRecords() throws Exception {
		/*
			Index two records - http://id.loc.gov/authorities/genreForms/gf2010025067, http://id.loc.gov/authorities/names/n50000589
		*/
		initUpdateTable(Arrays.asList("http://id.loc.gov/authorities/genreForms/gf2010025067", "http://id.loc.gov/authorities/names/n50000589"));
		IndexMadsAuthorityRecords.indexAllMadsAuthorityRecords(config, false);

		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing all: " + DbUtils.cursor(conn));
			assert "2026-08-26".equalsIgnoreCase(DbUtils.cursor(conn));
		}

		/*
			Index one new record - http://id.loc.gov/authorities/subjects/sh97007140
		 */
		initUpdateTable(Arrays.asList("http://id.loc.gov/authorities/subjects/sh97007140"));
		IndexMadsAuthorityRecords.indexNewMadsAuthorityRecords(config);

		try (Connection conn = config.getDatabaseConnection("Headings")) {
			System.out.println("Cursor after indexing new: " + DbUtils.cursor(conn));
			assert "2026-09-26".equalsIgnoreCase(DbUtils.cursor(conn));
		}
	}
}
