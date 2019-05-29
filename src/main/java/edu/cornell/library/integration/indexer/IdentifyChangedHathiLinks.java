package edu.cornell.library.integration.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.indexer.queues.AddToQueue;
import edu.cornell.library.integration.indexer.utilities.Config;

public class IdentifyChangedHathiLinks {


	public static void main(String[] args) throws FileNotFoundException, IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.add("hathiUpdatesFilesDirectory");
		Config config = Config.loadConfig(args, requiredArgs);

		new IdentifyChangedHathiLinks(config);

	}

	public IdentifyChangedHathiLinks(Config config)
			throws FileNotFoundException, IOException, SQLException {

		DateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar date = Calendar.getInstance();
		date.add(Calendar.DATE, -1);
		String filename = "hathi_upd_"+format.format(date.getTime())+".txt";
		System.out.println(config.getHathiUpdatesFilesDirectory()+File.separator+filename);

		try (BufferedReader br = new BufferedReader(new FileReader(
				config.getHathiUpdatesFilesDirectory()+File.separator+filename));
				Connection current = config.getDatabaseConnection("Current");
				PreparedStatement addToGenQ = AddToQueue.generationQueueStmt(current)) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("\\t");
				String source = fields[20];
				String sourceId = fields[6];
				if ( ! source.equals("COO") ) continue;
				AddToQueue.add2Queue(addToGenQ, Integer.valueOf(sourceId), 6,
						new Timestamp(date.getTimeInMillis()), "HATHILINKS updated");
				System.out.printf( "Queued b%s: %s\n",sourceId,fields[11]);
			}
		}
	}

}