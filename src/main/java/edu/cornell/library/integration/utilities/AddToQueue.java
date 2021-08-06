package edu.cornell.library.integration.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import edu.cornell.library.integration.voyager.IdentifyChangedRecords.ChangeType;

public class AddToQueue {

	public static PreparedStatement generationQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO generationQueue ( hrid, cause, priority, record_date ) VALUES (?, ?, ?, ?)");
	}

	public static PreparedStatement availabilityQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO availabilityQueue ( hrid, cause, priority, record_date ) VALUES (?, ?, ?, ?)");
	}

	public static PreparedStatement headingsQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO headingsQueue ( hrid, cause, priority, record_date ) VALUES (?, ?, ?, ?)");
	}

	public static PreparedStatement deleteQueueStmt( Connection current ) throws SQLException {
		return current.prepareStatement(
				"INSERT INTO deleteQueue ( hrid, cause, priority, record_date ) "+
				"VALUES (?, 'Record Deleted or Suppressed', 0, NOW())");
	}

	public static void add2QueueBatch(PreparedStatement stmt, int bib_id, Timestamp mod_date, ChangeType type) throws SQLException {
		stmt.setInt(1, bib_id);
		stmt.setString(2, type.toString());
		stmt.setInt(3, 8);
//		stmt.setInt(3, type.getPriority());
		stmt.setTimestamp(4, mod_date);
		stmt.addBatch();
	}

	public static void add2Queue(PreparedStatement stmt, String instanceHrid, int priority, Timestamp mod_date, String cause) throws SQLException {
		stmt.setString(1, instanceHrid);
		stmt.setString(2, cause);
		stmt.setInt(3, priority);
		stmt.setTimestamp(4, mod_date);
		stmt.executeUpdate();
	}

	public static void add2DeleteQueueBatch(PreparedStatement stmt, int bib_id) throws SQLException {
		stmt.setInt(1, bib_id);
		stmt.addBatch();
	}

}
