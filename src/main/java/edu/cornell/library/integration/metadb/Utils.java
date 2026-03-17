package edu.cornell.library.integration.metadb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.UUID;

public class Utils {

	static Timestamp getUpdateCursor(Connection inventory, String label) throws SQLException {
		try (PreparedStatement pstmt = inventory.prepareStatement(
						"SELECT current_to_date FROM updateCursor WHERE cursor_name = ?")) {
			pstmt.setString(1, label);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next())
					return rs.getTimestamp(1);
			}
		}
		// default to two days ago
		return new Timestamp(Calendar.getInstance().getTime().getTime()-(48*60*60*1000));
	}


	static void queueUpdateToCache(Connection inventory, UUID uuid, FolioType t, Cause c) throws SQLException {

		System.out.format("QUEUE %s %s (cause:%s)\n",t.toString(), uuid.toString(), c.toString());
		try (PreparedStatement stmt = inventory.prepareStatement(
				"INSERT INTO cacheUpdateQueue (type, uuid, cause) VALUES (?, ?, ?)")) {
			stmt.setString(1, t.toString().toLowerCase());
			stmt.setString(2, uuid.toString());
			stmt.setString(3, c.toString());
			stmt.executeUpdate();
		}
	}

	enum FolioType {
		INSTANCE ( "instanceFolio", "folio_inventory.instance"),
		HOLDING ( "holdingFolio", "folio_inventory.holdings_record"), 
		ITEM ("itemFolio", "folio_inventory.item"),
		BIB ("bibFolio", "folio_source_record.records_lb"); //TODO needs different metadb query

		final public String cacheTableName;
		final public String metadbTableName;
		private FolioType(String cacheTableName, String metadbTableName) {
			this.cacheTableName = cacheTableName;
			this.metadbTableName = metadbTableName;
		}
	}
	enum Cause { DELETED, ADDED, UPDATED; }

}
