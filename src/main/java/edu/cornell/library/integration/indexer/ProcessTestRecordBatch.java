package edu.cornell.library.integration.indexer;

import java.sql.ResultSet;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.BatchLogic;
import edu.cornell.library.integration.indexer.updates.IncrementalBibFileToSolr;
import edu.cornell.library.integration.indexer.updates.RetrieveUpdatesBatch;

/**
 * Process 100 records, and display the differences between the stored versions of
 * the Solr documents and those produced.
 */
public class ProcessTestRecordBatch {

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

		new ProcessTestRecordBatch(config);
	}

	public ProcessTestRecordBatch(SolrBuildConfig config) throws Exception {
		int batchSize = 100;

		String random = RandomStringUtils.randomAlphanumeric(12);
		config.setWebdavBaseUrl(config.getWebdavBaseUrl()+"/"+random);
		String localBaseFilePath = config.getLocalBaseFilePath();
		if (localBaseFilePath != null)
			config.setLocalBaseFilePath(localBaseFilePath+"/"+random);
		config.setTestMode( true );

		// Grab 100 records in test mode
		BatchLogic b = new BatchLogic() {
			public void startNewBatch() { }
			public int targetBatchSize() { return batchSize; }
			public boolean addQueuedItemToBatch(ResultSet rs,int currSize) {
				return true;
			}
			public int unqueuedTargetCount(int currSize) {
				return batchSize - currSize;
			}
			public boolean isTestMode() { return true; }
		};

		new RetrieveUpdatesBatch(config, b);
		new IncrementalBibFileToSolr(config);

	}
}
