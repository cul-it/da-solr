package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.cornell.library.integration.authority.mads.AuthorityDataMadsSimple;
import edu.cornell.library.integration.authority.mads.AuthorityDbUtils;
import edu.cornell.library.integration.authority.mads.MadsHeadingType;
import edu.cornell.library.integration.authority.mads.MadsRecordStatus;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;

public class ActivityStreamsUtils {
	public static final String CURSOR_TABLE = "madsAuthorityCursor";
	public static final String CURSOR_NAME = "madsAuthorityCursor";

	public static void addBatch(PreparedStatement insertStmt, AuthorityDataMadsSimple data, String addedDate) throws SQLException {
		insertStmt.setString(1, data.id);
		insertStmt.setString(2, data.lccn);
		insertStmt.setInt(3, data.vocab.ordinal());
		insertStmt.setInt(4, data.recordStatus.ordinal());
		insertStmt.setString(5, data.authorativeLabel);
		insertStmt.setInt(6, data.headingType.ordinal());
		insertStmt.setBoolean(7, data.isSubdivision);
		insertStmt.setBoolean(8, data.undifferentiated);
		insertStmt.setString(9, data.moddate);
		insertStmt.setString(10, addedDate);
		insertStmt.setInt(11, data.numUpdates);
		insertStmt.setString(12, data.source);
		insertStmt.addBatch();
	}

	public static PreparedStatement existsStmt(Connection authorityDB) throws SQLException {
		return authorityDB.prepareStatement(
				"SELECT 1 FROM %s WHERE id = ? AND numUpdates = ? AND moddate = ? LIMIT 1".formatted(AuthorityDbUtils.MADS_UPDATE_TABLE));
	}

	public static boolean exists(PreparedStatement checkStmt, AuthorityDataMadsSimple toCheck) throws SQLException {
		checkStmt.setString(1, toCheck.id);
		checkStmt.setInt(2, toCheck.numUpdates);
		checkStmt.setString(3, toCheck.moddate);
		try (ResultSet rs = checkStmt.executeQuery()) {
			if (rs.next())
				return true;
		}
		return false;
	}

	public static boolean alreadyProcessed(Map<String, Boolean> seen, AuthorityDataMadsSimple toCheck) {
		String key = toCheck.id + toCheck.moddate + toCheck.numUpdates;
		if (seen.containsKey(key)) {
			return true;
		} else {
			seen.put(key,  true);
			return false;
		}
	}

	public static String getDestName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String path = uri.getPath();
		String basename = path.substring(path.lastIndexOf('/') + 1);
		if (!basename.endsWith(".gz"))
			throw new IllegalArgumentException("Input is not gzip! " + url);

		return basename.substring(0, basename.length() - 3);
	}

	public static String getStackTraceAsString(Throwable throwable) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		throwable.printStackTrace(pw);
		return sw.toString();
	}

	public static String getToday() {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return now.format(formatter);
	}

	public static PreparedStatement replaceStmt(Connection authorityDB) throws SQLException {
		return authorityDB.prepareStatement(
				"""
REPLACE INTO %s (id,lccn,vocabulary,recordStatus,heading,headingType,isSubdivision,undifferentiated,moddate,addedDate,numUpdates,source)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""".formatted(AuthorityDbUtils.MADS_UPDATE_TABLE));
	}

	public static void setUpDatabase(Connection authority) throws SQLException {
		List<String> sqls = Arrays.asList(
			// Authority tables
			"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` VARCHAR(210) NOT NULL,
	`lccn` VARCHAR(50) DEFAULT NULL,
	`vocabulary` TINYINT(4) DEFAULT NULL,
	`recordStatus` TINYINT(1) DEFAULT NULL,
	`heading` TEXT DEFAULT NULL,
	`headingType` TINYINT(4) DEFAULT NULL,
	`isSubdivision` TINYINT(1) DEFAULT NULL,
	`undifferentiated` TINYINT(1) DEFAULT NULL,
	`moddate` VARCHAR(30) DEFAULT NULL,
	`addedDate` VARCHAR(30) DEFAULT NULL,
	`numUpdates` INT DEFAULT 0,
	`source` JSON NOT NULL,
	UNIQUE KEY `unique_id` (`id`, `numUpdates`, `moddate`),
	KEY `idx_id` (`id`),
	KEY `idx_added_date` (`addedDate`),
	KEY `idx_heading` (heading(255)),
	KEY `idx_moddate` (`moddate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(AuthorityDbUtils.MADS_UPDATE_TABLE),
			"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` TINYINT(3) unsigned NOT NULL,
	`name` VARCHAR(256) NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=MyISAM""".formatted(AuthorityDbUtils.MADS_HEADING_TYPE_TABLE),
			"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` TINYINT(1) unsigned NOT NULL,
	`name` VARCHAR(256) NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=MyISAM""".formatted(AuthorityDbUtils.MADS_RECORD_STATUS_TABLE),
			"""
CREATE TABLE IF NOT EXISTS `%s` (
	`cursor_name` varchar(25) NOT NULL,
	`current_to_date` varchar(30) DEFAULT NULL,
	PRIMARY KEY (`cursor_name`))
	ENGINE=MyISAM DEFAULT CHARSET=utf8""".formatted(CURSOR_TABLE)
);

		try ( Statement stmt = authority.createStatement() ) {
			for (String sql : sqls)
				stmt.execute(sql);
		}

		try ( PreparedStatement insertDesc = authority.prepareStatement(
				"REPLACE INTO %s (id,name) VALUES (? , ?)".formatted(AuthorityDbUtils.MADS_HEADING_TYPE_TABLE)) ) {
		for ( MadsHeadingType ht : MadsHeadingType.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertDesc = authority.prepareStatement(
				"REPLACE INTO %s (id,name) VALUES (? , ?)".formatted(AuthorityDbUtils.MADS_RECORD_STATUS_TABLE)) ) {
		for ( MadsRecordStatus rs : MadsRecordStatus.values()) {
			insertDesc.setInt(1, rs.ordinal());
			insertDesc.setString(2, rs.toString());
			insertDesc.executeUpdate();
		}}
	}

	public static String cursor(Connection heading) throws SQLException {
		try ( PreparedStatement pstmt = heading.prepareStatement("SELECT current_to_date FROM %s WHERE cursor_name = ?".formatted(CURSOR_TABLE))) {
			pstmt.setString(1, CURSOR_NAME);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next())
					return rs.getString(1);
			}
		}
		return null;
	}

	public static void cursorReplace(Connection heading, String cursor) throws SQLException {
		try ( PreparedStatement pstmt = heading.prepareStatement("REPLACE INTO %s (cursor_name, current_to_date) VALUES (?, ?)".formatted(CURSOR_TABLE))) {
			pstmt.setString(1, CURSOR_NAME);
			pstmt.setString(2, cursor);
			pstmt.executeUpdate();
		}
	}

	public static void printJsonLd(JsonObject doc) throws IOException {
		try (StringWriter sw = new StringWriter();
			 JsonWriter jsonWriter = Json.createWriter(sw)) {
			jsonWriter.write(doc);
			System.out.println(sw.toString());
		}
	}
}
