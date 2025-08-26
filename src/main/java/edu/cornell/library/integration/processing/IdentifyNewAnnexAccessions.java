package edu.cornell.library.integration.processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.utilities.AddToQueue;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyNewAnnexAccessions {

	public static void main(String[] args) throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Voy"));
		requiredArgs.add("annexFlipsUrl");
		Config config = Config.loadConfig(requiredArgs);
		DateFormat format = new SimpleDateFormat("yyyy_MM_dd");
		Calendar date = Calendar.getInstance();
		date.add(Calendar.DATE, -1);
		URL url = new URL(config.getAnnexFlipsUrl()+"/barcodes_from_annex_"+format.format(date.getTime()));
		List<String> barcodes = new ArrayList<>();
		int barcodeCount = 0;
		try(BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {

			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				barcodes.add(inputLine.replaceAll("[^\\d]", ""));
				barcodeCount++;
			}
		}
		if (barcodes.isEmpty()) {
			System.out.println("No Annex flips yesterday.");
			System.exit(0);
		}
		List<List<String>> barcodeChunks = spliterateBarcodeList(barcodes);
		int bibCount = 0;
		try ( Connection voyager = config.getDatabaseConnection("Voy");
				Statement stmt = voyager.createStatement();
				Connection current = config.getDatabaseConnection("Current");
				PreparedStatement availQ = AddToQueue.availQueueStmt(current)) {
			for (List<String> barcodeChunk : barcodeChunks) {
				String query = "SELECT DISTINCT bib_id FROM item_barcode, mfhd_item, bib_mfhd WHERE item_barcode in ('";
				query += String.join("', '", barcodeChunk);
				query += "') AND item_barcode.item_id = mfhd_item.item_id AND bib_mfhd.mfhd_id = mfhd_item.mfhd_id";
				System.out.println(query);
				try ( ResultSet rs = stmt.executeQuery(query) ) {
					while ( rs.next() ) {
						System.out.println("Queueing "+rs.getString(1));
						AddToQueue.add2Queue(availQ, rs.getString(1), 8, new Timestamp(date.getTimeInMillis()),"AnnexFlips");
						bibCount++;
					}
				}
			}
		}
		System.out.printf("Queued %d bibs for update based on flips of %d barcodes.\n",bibCount,barcodeCount);
	}

	private static List<List<String>> spliterateBarcodeList( List<String> barcodes ) {
		List<String> toSplits = barcodes;
		List<List<String>> splits = new ArrayList<>();
		while ( toSplits.size() > 1000 ) {
			splits.add(barcodes.subList(0, 999));
			toSplits = toSplits.subList(1000, toSplits.size()-1);
		}
		splits.add(toSplits);
		return splits;
	}
}
