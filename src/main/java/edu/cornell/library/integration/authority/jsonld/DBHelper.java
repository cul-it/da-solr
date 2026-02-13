package edu.cornell.library.integration.authority.jsonld;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class DBHelper {
	public static final String CURSOR_TABLE = "authorityJsonLDUpdateCursor";
	public static final String SOURCE_TABLE = "authoritySourceJsonLD";
	public static final String UPDATE_TABLE = "authorityUpdateJsonLD";
	
	protected static String getCursor(Connection authority) throws SQLException {
		String sql = "SELECT `cursor` FROM %s WHERE `cursor_name` = 'authority_activity_streams'".formatted(CURSOR_TABLE);
		try ( Statement stmt = authority.createStatement();
			  ResultSet rs = stmt.executeQuery(sql)) {
			if (rs.next()) return rs.getString(1);

			throw new SQLException("authorityJsonLDUpdateCursor table is empty!");
		}
	}

	protected static String updateCursor(Connection authority, String cursor) throws SQLException {
		String sql = "REPLACE INTO %s (`cursor_name`, `cursor`) VALUES ('authority_activity_streams', ?)".formatted(CURSOR_TABLE);
		try (PreparedStatement pstmt = authority.prepareStatement(sql)) {
			pstmt.setString(1, cursor);
			pstmt.execute();
		}
		return cursor;
	}

	protected static void setUpDatabase(Connection authority) throws SQLException {
		List<String> sqls = Arrays.asList(
				"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` int(10) unsigned NOT NULL auto_increment,
	`json` text,
	PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".formatted(SOURCE_TABLE),
				"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` varchar(255) NOT NULL,
	`lccn` varchar(20) DEFAULT NULL,
	`vocabulary` tinyint(4) DEFAULT NULL,
	`recordStatus` tinyint(1) DEFAULT NULL,
	`heading` text DEFAULT NULL,
	`headingType` tinyint(4) DEFAULT NULL,
	`isSubdivision` tinyint(1) DEFAULT NULL,
	`undifferentiated` tinyint(1) DEFAULT NULL,
	`moddate` varchar(30) DEFAULT NULL,
	`rawId` int DEFAULT NULL,
	UNIQUE KEY `unique_id` (`id`, `moddate`),
	KEY `key_id` (`id`),
	FOREIGN KEY (rawId) REFERENCES %s(id) ON DELETE SET NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".formatted(UPDATE_TABLE, SOURCE_TABLE),
	"""
CREATE TABLE `%s` (
	`cursor_name` varchar(30) NOT NULL,
	`cursor` varchar(30) NOT NULL,
	PRIMARY KEY (`cursor_name`)) ENGINE=MyISAM DEFAULT CHARSET=utf8""".formatted(CURSOR_TABLE));
		try ( Statement stmt = authority.createStatement() ) {
			for (String sql : sqls)
				stmt.execute(sql);
		}
	}

	protected static PreparedStatement getInsertStmt(Connection authorityDB) throws SQLException {
		return authorityDB.prepareStatement(
				"""
REPLACE INTO %s (id,lccn,vocabulary,recordStatus,heading,headingType,isSubdivision,undifferentiated,moddate,rawId)
	VALUES (?,?,?,?,?,?,?,?,?,?)""".formatted(UPDATE_TABLE));
	}

	protected static void addBatch(PreparedStatement insertStmt, AuthorityParsedData data, int rawId) throws SQLException {
		insertStmt.setString(1, data.id);
		insertStmt.setString(2, data.lccn);
		insertStmt.setInt(3, data.vocab.ordinal());
		insertStmt.setInt(4, data.recordStatus.ordinal());
		insertStmt.setString(5, data.authorativeLabel);
		insertStmt.setInt(6, data.headingType.ordinal());
		insertStmt.setBoolean(7, data.isSubdivision);
		insertStmt.setBoolean(8, data.undifferentiated);
		insertStmt.setString(9, data.moddate);
		insertStmt.setInt(10, rawId);
		insertStmt.addBatch();
	}
}
