package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.utilities.IndexingUtilities.commitIndexChanges;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.BatchLogic;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class IterativeUpdateFromVoyager {

	private final static String isQueueRemainingQuery = "SELECT * FROM "+CurrentDBTable.QUEUE+
			" WHERE done_date = 0 AND batched_date = 0 AND priority = 0 LIMIT 1";

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("dailyMrcDir");
		requiredArgs.add("dailyMfhdDir");
		requiredArgs.add("marc2XmlDirs");
		requiredArgs.add("dailyBibMrcXmlDir");
		requiredArgs.add("dailyMfhdMrcXmlDir");
		requiredArgs.add("dailyMrcNtDir");
		requiredArgs.add("dailyMrcNtFilenamePrefix");
    	requiredArgs.add("solrUrl");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(args,requiredArgs);

		config.setTargetBatchSize(1_000);

		String webdavBaseURL = config.getWebdavBaseUrl();
		String localBaseFilePath = config.getLocalBaseFilePath();
		config.setDatabasePoolsize("Current", 2);
		Integer quittingTime = config.getEndOfIterativeCatalogUpdates();
		if (quittingTime == null) quittingTime = 19;
		System.out.println("Processing updates to Voyager until: "+quittingTime+":00.");
		boolean timeToQuit = false;

		BatchLogic b = new BatchLogic() {
			public boolean addQueuedItemToBatch(ResultSet rs) { return true; }
			public int targetBatchSize() { return 500; }
			public int unqueuedTargetCount(int currentBatchSize) {
				if (currentBatchSize == 0) return 500;
				if (currentBatchSize >= 300) return 0;
				return 300 - currentBatchSize;
			}
		};

		int i = 0;
		while ( ! timeToQuit || isQueueRemaining(config) ) {
			if ( ! timeToQuit ) {
				if ( Calendar.getInstance().get(Calendar.HOUR_OF_DAY) == quittingTime)
					timeToQuit = true;
			} else {
				// If four hours past quitting time, cancel quit (run straight through to the next day).
				if ( (24 + Calendar.getInstance().get(Calendar.HOUR_OF_DAY) - 4) % 24 == quittingTime)
					timeToQuit = false;
			}
			config.setWebdavBaseUrl(webdavBaseURL + "/" + (++i) );
			config.setLocalBaseFilePath(localBaseFilePath + "/" + i);
			new IdentifyChangedRecords(config,false);
			DeleteFromSolr.doTheDelete(config);
			new RetrieveUpdatesBatch(config, b);
			new IncrementalBibFileToSolr(config);
			commitIndexChanges( config.getSolrUrl() );
		}
	}

	private static boolean isQueueRemaining(SolrBuildConfig config) throws ClassNotFoundException, SQLException {
		boolean remaining = false;
		try ( Connection current = config.getDatabaseConnection("Current");
				Statement stmt = current.createStatement();
				ResultSet rs = stmt.executeQuery( isQueueRemainingQuery )) {
			while (rs.next())
				remaining = true;
		}
		return remaining;
	}

}
