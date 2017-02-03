package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
		int batchSize = 1200;

		String random = RandomStringUtils.randomAlphanumeric(12);
		String webdavBaseUrl = config.getWebdavBaseUrl()+"/"+random+"/";
		String localBaseFilePath = config.getLocalBaseFilePath();
		if (localBaseFilePath != null)
			localBaseFilePath += "/"+random+"/";
		BatchLogic b = new BatchLogic() {
			private boolean priorityZeroInBatch = false;
			public int targetBatchSize() {
				priorityZeroInBatch = false;
				return batchSize;
			}
			public boolean addQueuedItemToBatch(ResultSet rs) throws SQLException {
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

		try ( Connection current = config.getDatabaseConnection("Current") ){
			int i = 0;
			while (i < 20) {
				config.setWebdavBaseUrl(webdavBaseUrl+(++i));
				if (localBaseFilePath != null)
					config.setLocalBaseFilePath(localBaseFilePath+i);
				new RetrieveUpdatesBatch(config, b);
				new IncrementalBibFileToSolr(config);
			}
		}
	}
}
