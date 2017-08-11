package edu.cornell.library.integration.indexer.updates;

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

		String webdavBaseURL = config.getWebdavBaseUrl();
		String localBaseFilePath = config.getLocalBaseFilePath();
		config.setDatabasePoolsize("Current", 2);
		Integer quittingTime = config.getEndOfIterativeCatalogUpdates();
		if (quittingTime == null) quittingTime = 19;
		System.out.println("Processing updates to Voyager until: "+quittingTime+":00.");
		boolean timeToQuit = false;

		int batchSizeIfUpdates = 200;
		int batchSizeIfNoUpdates = 300;
		/* If any priority 0 or 1 items are queued, lower priority items will only be accepted to
		 * the point where the total batch is $batchSizeIfUpdates items. Otherwise, the default size of
		 * $batchSizeIfNoUpdates items is the limit.
		 */
		BatchLogic b = new BatchLogic() {
			Integer topPriority = null;
			public void startNewBatch() {
				topPriority = null;
			}
			public int targetBatchSize() { return batchSizeIfNoUpdates; }
			public boolean addQueuedItemToBatch(ResultSet rs,int currentBatchSize) throws SQLException {
				if (topPriority == null)
					topPriority = rs.getInt("priority");
				if (currentBatchSize < batchSizeIfUpdates)
					return true;
				if (topPriority <= 1) {
					rs.afterLast();
					return false;
				}
				return true;
			}
			public int unqueuedTargetCount(int currentBatchSize) {
				if (currentBatchSize == 0) return batchSizeIfNoUpdates;
				if (currentBatchSize >= batchSizeIfUpdates) return 0;
				return batchSizeIfUpdates - currentBatchSize;
			}
			public boolean isTestMode() { return false; }
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
