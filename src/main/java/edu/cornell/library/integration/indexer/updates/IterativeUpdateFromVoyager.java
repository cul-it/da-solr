package edu.cornell.library.integration.indexer.updates;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.indexer.queues.BatchRecordsForSolrIndex.BatchLogic;
import edu.cornell.library.integration.indexer.utilities.Config;

public class IterativeUpdateFromVoyager {

	private final static String isQueueRemainingQuery = "SELECT * FROM indexQueue"+
			" WHERE done_date = 0 AND batched_date = 0 AND priority = 0 LIMIT 1";

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(Config.getRequiredArgsForWebdav());
		requiredArgs.add("dailyMrcDir");
		requiredArgs.add("dailyMfhdDir");
		requiredArgs.add("marc2XmlDirs");
		requiredArgs.add("dailyBibMrcXmlDir");
		requiredArgs.add("dailyMfhdMrcXmlDir");
		requiredArgs.add("dailyMrcNtDir");
		requiredArgs.add("dailyMrcNtFilenamePrefix");
    	requiredArgs.add("solrUrl");
		Config config = Config.loadConfig(args,requiredArgs);

		String webdavBaseURL = config.getWebdavBaseUrl();
		String localBaseFilePath = config.getLocalBaseFilePath();
		config.setDatabasePoolsize("Current", 2);
		Integer quittingTime = config.getEndOfIterativeCatalogUpdates();
		if (quittingTime == null) quittingTime = 19;
		System.out.println("Looking for updates to Voyager until: "+quittingTime+":00.");
		boolean timeToQuit = false;

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
			Thread.sleep(250);
		}
	}

	private static boolean isQueueRemaining(Config config) throws ClassNotFoundException, SQLException {
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
