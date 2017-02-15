package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.IndexingUtilities.commitIndexChanges;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.BatchLogic;
import edu.cornell.library.integration.indexer.updates.IncrementalBibFileToSolr;
import edu.cornell.library.integration.indexer.updates.RetrieveUpdatesBatch;
import edu.cornell.library.integration.utilities.IndexingUtilities.IndexQueuePriority;

public class ProcessQueue {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("solrUrl");
		requiredArgs.add("dailyBibMrcXmlDir");
		requiredArgs.add("dailyMfhdMrcXmlDir");
		requiredArgs.add("dailyMrcNtDir");
		requiredArgs.add("dailyMrcNtFilenamePrefix");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(args, requiredArgs);

		new ProcessQueue(config);
	}

	public ProcessQueue(SolrBuildConfig config) throws Exception {
		int batchSize = 600;

		String random = RandomStringUtils.randomAlphanumeric(12);
		String webdavBaseUrl = config.getWebdavBaseUrl()+"/"+random+"/";
		String localBaseFilePath = config.getLocalBaseFilePath();
		if (localBaseFilePath != null)
			localBaseFilePath += "/"+random+"/";

		/* If any priority 0 items are queued, no other items will be accepted into the queue even
		 * if the total queue size ends up below the default batchSize.
		 */
		BatchLogic b = new BatchLogic() {
			private boolean priorityZeroInBatch = false;
			public void startNewBatch() {
				priorityZeroInBatch = false;
			}
			public int targetBatchSize() { return batchSize; }
			public boolean addQueuedItemToBatch(ResultSet rs,int currentBatchSize) throws SQLException {
				if ( rs.getInt("priority") == IndexQueuePriority.DATACHANGE.ordinal() ) {
					priorityZeroInBatch = true;
					return true;
				}
				if (priorityZeroInBatch) {
					rs.afterLast();
					return false;
				}
				return true;
			}
			public int unqueuedTargetCount(int currentBatchSize) {
				if (priorityZeroInBatch) return 0;
				return batchSize - currentBatchSize;
			}
		};

		Integer quittingTime = config.getEndOfIterativeCatalogUpdates();
		if (quittingTime == null) quittingTime = 19;
		System.out.println("Processing Voyager records until: "+quittingTime+":00.");
		int i = 0;
		while (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) != quittingTime) {
			config.setWebdavBaseUrl(webdavBaseUrl+(++i));
			if (localBaseFilePath != null)
				config.setLocalBaseFilePath(localBaseFilePath+i);
			new RetrieveUpdatesBatch(config, b);
			new IncrementalBibFileToSolr(config);
		}
		commitIndexChanges( config.getSolrUrl() );
	}
}
