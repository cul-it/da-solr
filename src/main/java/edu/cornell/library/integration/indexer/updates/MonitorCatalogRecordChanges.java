package edu.cornell.library.integration.indexer.updates;

import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.voyager.IdentifyChangedRecords;

public class MonitorCatalogRecordChanges {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Voy"));
		Config config = Config.loadConfig(requiredArgs);

		Integer quittingTime = config.getEndOfIterativeCatalogUpdates();
		if (quittingTime == null) quittingTime = 19;
		System.out.println("Looking for updates to Voyager until: "+quittingTime+":00.");

		while ( Calendar.getInstance().get(Calendar.HOUR_OF_DAY) != quittingTime ) {
			new IdentifyChangedRecords(config,false);
			Thread.sleep(250);
		}
	}

}
