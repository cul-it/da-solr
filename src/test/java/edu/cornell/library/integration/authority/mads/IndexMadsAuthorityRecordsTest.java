package edu.cornell.library.integration.authority.mads;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.cornell.library.integration.authority.activitystreams.IFetcher;
import edu.cornell.library.integration.authority.activitystreams.Utils;
import edu.cornell.library.integration.db_test.CustomDbSetupTest;

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
		testContainersSetupFunctions.add(conn -> {
			try {
				Utils.setUpDatabase(conn);
				DbUtils.setUpHeadingsDatabase(conn);
				DbUtils.setupAuthorityDatabase(conn);
				return null;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		});
		Path extraInit = Path.of(base, "authority_extra_insert.sql");
		extraInsertSqls = Files.readAllLines(extraInit);
		custom_setup();

		DbUtils.fetcher = new IFetcher.FileFetcher(rootPath);
	}

	@Test
	public void testIndexAllMadsAuthorityRecords() throws Exception {
		IndexMadsAuthorityRecords.indexAllMadsAuthorityRecords(config, false);
	}
}
