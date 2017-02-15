package edu.cornell.library.integration.indexer.updates;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import edu.cornell.library.integration.utilities.IndexingUtilities.IndexQueuePriority;

public class QueueRecordsForSolrIndex {

	public static String usage = "QueueRecordsForSolrIndex <level> <cause> <inputfile>\n"
			+ "<level> : 2, 3, 4\n"
			+ "<cause> : e.g. 'Add new field for xyzzy'\n"
			+ "<inputfile> : path to text file containing list of affected bibs.";
	public static void main(String[] args) {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		try {
			new QueueRecordsForSolrIndex( SolrBuildConfig.loadConfig(null,requiredArgs),args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public QueueRecordsForSolrIndex(SolrBuildConfig config, String[] args) throws Exception { 
		if (args.length != 3) {
			throw new IllegalArgumentException(usage + "("+String.valueOf(args.length)+")");
		}
		IndexQueuePriority priority = null;
		switch (args[0]) {
		case "2": priority = IndexQueuePriority.CODECHANGE_PRIORITY2; break;
		case "3": priority = IndexQueuePriority.CODECHANGE_PRIORITY3; break;
		case "4": priority = IndexQueuePriority.CODECHANGE_PRIORITY4; break;
		default:
			throw new IllegalArgumentException("First argument must be 2, 3, or 4, where 2 is the highest allowable priority.\n\n"+usage);
		}
		String cause = args[1];
		if (cause.length() > 256)
			throw new IllegalArgumentException("Second argument must be a short description (<= 256 bytes) of the cause for the update.\n\n"+usage);

		String filename = args[2];
		int recCount = 0;

		try (   Connection conn = config.getDatabaseConnection("Current");
				PreparedStatement pstmt = conn.prepareStatement(
						"INSERT INTO "+CurrentDBTable.QUEUE.toString()
						+" (bib_id, cause, priority) VALUES (?, ?, ?)") ){

			pstmt.setInt(3, priority.ordinal());
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
