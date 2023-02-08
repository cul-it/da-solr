package edu.cornell.library.integration.db_test;

import java.io.IOException;
import java.util.List;

import edu.cornell.library.integration.utilities.Config;

/*
 * Requirements to run these tests with local DB:
 * 1. Docker must be installed on the machine running these tests.
 * 2. UseTestContainers and VoyagerToSolrConfig env vars need to be set.
 * 
 * UseTestContainers=true
 * 
 * - As long as UseTestContainers variable is set with any value, DB container will start.
 * - If UseTestContainers is not set, it will use the VoyagerToSolrConfig to connect to DB directly.
 * 
 * VoyagerToSolrConfig=/DA-SOLR-PATH/src/test/resources/example_db_data/folio-unit-test.properties
 * 
 * - For VoyagerToSolrConfig, /DA-SOLR-PATH needs to be set to where your your da-solr project reside.
 * 
 */
public class DbBaseTest {
	protected static Config config = null;
	protected static final String USE_TEST_CONTAINERS = "UseTestContainers";
	protected static String useTestContainers = null;

	public static void setup(String dbName) throws IOException {
		useTestContainers = System.getenv(USE_TEST_CONTAINERS);
		List<String> requiredArgs = Config.getRequiredArgsForDB(dbName);
		if (useTestContainers != null) {
			AbstractContainerBaseTest.setProperties();
		}
		config = Config.loadConfig(requiredArgs);
	}
}
