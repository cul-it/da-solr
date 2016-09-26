package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.utilities.IndexingUtilities.commitIndexChanges;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
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
		int quittingTime = ( config.getExtendedIndexingMode() ) ? 23 : 18;
		System.out.println("Processing updates to Voyager until: "+quittingTime+":00.");

		int i = 0;
		while (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) < quittingTime
				|| isQueueRemaining(config) ) {

			config.setWebdavBaseUrl(webdavBaseURL + "/" + (++i) );
			config.setLocalBaseFilePath(localBaseFilePath + "/" + i);
			new IdentifyChangedRecords(config,false);
			DeleteFromSolr.doTheDelete(config);
			new RetrieveUpdatesBatch(config);

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
