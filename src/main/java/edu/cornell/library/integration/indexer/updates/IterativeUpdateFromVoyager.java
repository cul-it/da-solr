package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.utilities.IndexingUtilities.commitIndexChanges;

import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

public class IterativeUpdateFromVoyager {
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

		switch (Calendar.getInstance().get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SATURDAY:
		case Calendar.SUNDAY:
			config.setTargetBatchSize(5_000); break;
		default: config.setTargetBatchSize(1_000);
		}

		String webdavBaseURL = config.getWebdavBaseUrl();
		String localBaseFilePath = config.getLocalBaseFilePath();
		config.setDatabasePoolsize("Current", 2);
		int quittingTime = ( config.getExtendedIndexingMode() ) ? 23 : 18;
		System.out.println("Processing updates to Voyager until: "+quittingTime+":00.");

		int i = 0;
		while (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) < quittingTime) {

			config.setWebdavBaseUrl(webdavBaseURL + "/" + (++i) );
			config.setLocalBaseFilePath(localBaseFilePath + "/" + i);
			new IdentifyChangedRecords(config,false);
			DeleteFromSolr dfs = new DeleteFromSolr();
			dfs.doTheDelete(config);
			new RetrieveUpdatesBatch(config);

			new IncrementalBibFileToSolr(config);
			commitIndexChanges( config.getSolrUrl() );
		}
	}

}
