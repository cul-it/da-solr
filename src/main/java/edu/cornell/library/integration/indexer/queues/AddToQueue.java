package edu.cornell.library.integration.indexer.queues;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class AddToQueue {

	public static PreparedStatement generationQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO generationQueue ( bib_id, cause, priority, record_date ) VALUES (?, ?, ?, ?)");
	}

	public static PreparedStatement availabilityQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO availabilityQueue ( bib_id, cause, priority, record_date ) VALUES (?, ?, ?, ?)");
	}

	public static void add2QueueBatch(PreparedStatement stmt, int bib_id, Timestamp mod_date, String cause) throws SQLException {
		stmt.setInt(1, bib_id);
		stmt.setString(2, cause);
		stmt.setInt(3, 0);
		stmt.setTimestamp(4, mod_date);
		stmt.addBatch();
	}

	public static void add2Queue(PreparedStatement stmt, int bib_id, int priority, Timestamp mod_date, String cause) throws SQLException {
		stmt.setInt(1, bib_id);
		stmt.setString(2, cause);
		stmt.setInt(3, 5);
		stmt.setTimestamp(4, mod_date);
		stmt.executeUpdate();
	}
}
