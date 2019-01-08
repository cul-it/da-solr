package edu.cornell.library.integration.processing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import edu.cornell.library.integration.utilities.Config;

public class QueueRecordsForSolrIndex {

	public static String usage = "QueueRecordsForSolrIndex <level> <cause> <inputfile>\n"
			+ "<level> : 2, 3, 4\n"
			+ "<cause> : e.g. 'Add new field for xyzzy'\n"
			+ "<inputfile> : path to text file containing list of affected bibs.";
	public static void main(String[] args) {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		try {
			new QueueRecordsForSolrIndex( Config.loadConfig(null,requiredArgs),args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public QueueRecordsForSolrIndex(Config config, String[] args) throws Exception { 
		if (args.length != 3) {
			throw new IllegalArgumentException(usage + "("+String.valueOf(args.length)+")");
		}
		Integer priority ;
		if (args[0].equals("2") || args[0].equals("3") || args[0].equals("4"))
			priority = Integer.valueOf( args[0] );
		else 
			throw new IllegalArgumentException("First argument must be 2, 3, or 4, where 2 is the highest allowable priority.\n\n"+usage);
		String cause = args[1];
		if (cause.length() > 256)
			throw new IllegalArgumentException("Second argument must be a short description (<= 256 bytes) of the cause for the update.\n\n"+usage);

		String filename = args[2];
		int recCount = 0;

		try (   Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement(
						"INSERT INTO indexQueue (bib_id, cause, priority) VALUES (?, ?, ?)") ){

			pstmt.setInt(3, priority);
			pstmt.setString(2, cause);

			try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
				String line;
				while ((line = br.readLine()) != null) {
					pstmt.setInt(1, Integer.valueOf(line));
					pstmt.addBatch();
					if (++recCount % 1000 == 0)
						pstmt.executeBatch();
				}
			}
			pstmt.executeBatch();
		}
		System.out.println("Queued "+recCount+" bib records for re-index with priority "
				+priority.toString()+" and reason '"+cause+"'");
	}

}
