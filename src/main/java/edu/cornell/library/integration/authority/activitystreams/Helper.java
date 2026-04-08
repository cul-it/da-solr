package edu.cornell.library.integration.authority.activitystreams;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;

public class Helper {
	public static final String UPDATE_TABLE = "authorityUpdateActivityStreams";

	public static void addBatch(PreparedStatement insertStmt, AuthorityParsedData data, String addedDate, String source) throws SQLException {
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
		insertStmt.setString(12, source);
		insertStmt.addBatch();
	}

	public static String getToday() {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return now.format(formatter);
	}

	public static List<String> getIdAsList(JsonValue arg) {
		List<String> ids = new ArrayList<>();
		if (arg.getValueType() == ValueType.ARRAY) {
			for (JsonValue val : arg.asJsonArray())
				ids.add(val.asJsonObject().getJsonString("@id").getString());
		} else if (arg.getValueType() == ValueType.OBJECT)
			ids.add(arg.asJsonObject().getJsonString("@id").getString());
		return ids;
	}

	public static JsonObject getJsonObjectForId(JsonArray graph, String id) {
		for (JsonValue entry : graph) {
			JsonObject obj = entry.asJsonObject();
			String entryId = obj.getString("@id");
			if (id.equalsIgnoreCase(entryId))
				return obj;
		}
		return JsonValue.EMPTY_JSON_OBJECT;
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

	// Get string value or null if key doesn't exist or not a string type
	public static String getString(JsonObject obj, String key) {
		JsonValue value = obj.get(key);
		if (value == null)
			return null;
		if (value.getValueType() == JsonValue.ValueType.STRING)
			return ((JsonString) value).getString();
		return null;
	}

	/*
	 * Shorthand to get string value from a generic type JsonValue
	 * If it is an object, return the @value as string
	 */
	public static String getString(JsonValue value) {
		if (value.getValueType() == ValueType.STRING)
			return ((JsonString) value).getString();
		else if (value.getValueType() == ValueType.OBJECT)
			return value.asJsonObject().getJsonString("@value").getString();
		return null;
	}

	public static PreparedStatement replaceStmt(Connection authorityDB) throws SQLException {
		return authorityDB.prepareStatement(
				"""
REPLACE INTO %s (id,lccn,vocabulary,recordStatus,heading,headingType,isSubdivision,undifferentiated,moddate,addedDate,numUpdates,source)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""".formatted(UPDATE_TABLE));
	}

	public static void setUpDatabase(Connection authority) throws SQLException {
		List<String> sqls = Arrays.asList(
				"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` VARCHAR(255) NOT NULL,
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
	UNIQUE KEY `unique_id` (`id`, `numUpdates`),
	KEY `idx_id` (`id`),
	KEY `idx_added_date` (`addedDate`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".formatted(UPDATE_TABLE));
		try ( Statement stmt = authority.createStatement() ) {
			for (String sql : sqls)
				stmt.execute(sql);
		}
	}
}
