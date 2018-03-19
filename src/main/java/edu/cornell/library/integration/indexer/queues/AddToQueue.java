package edu.cornell.library.integration.indexer.queues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class AddToQueue {

	public static void newBib(Config config, int bib_id, Timestamp mod_date)
			throws ClassNotFoundException, SQLException {

		try (
				Connection current = config.getDatabaseConnection("Current");
				PreparedStatement stmt = current.prepareStatement(
						"INSERT INTO "+CurrentDBTable.GEN_Q+" ( bib_id, cause, priority, record_date )"+
					" VALUES (?, ?, ?, ?)");){

			stmt.setInt(1, bib_id);
			stmt.setString(2, "New Bibliographic Record");
			stmt.setInt(3, 0);
			stmt.setTimestamp(4, mod_date);
			stmt.execute();
			
		}
	}

}
