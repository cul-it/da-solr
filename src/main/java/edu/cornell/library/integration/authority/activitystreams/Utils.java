package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.AuthoritySource;
import edu.cornell.library.integration.authority.mads.AuthorityMap;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import jakarta.json.JsonWriter;

public class Utils {
	public static final String HEADING_TYPE_TABLE = "madsAuthorityHeadingType";
	public static final String RECORD_STATUS_TABLE = "madsAuthorityRecordStatus";
	public static final String UPDATE_TABLE = "madsAuthorityUpdate";
	private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();

	public static void addBatch(PreparedStatement insertStmt, AuthorityParsedData data, String addedDate) throws SQLException {
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
				"SELECT 1 FROM %s WHERE id = ? AND numUpdates = ? AND moddate = ? LIMIT 1".formatted(UPDATE_TABLE));
	}

	public static boolean exists(PreparedStatement checkStmt, AuthorityParsedData toCheck) throws SQLException {
		checkStmt.setString(1, toCheck.id);
		checkStmt.setInt(2, toCheck.numUpdates);
		checkStmt.setString(3, toCheck.moddate);
		try (ResultSet rs = checkStmt.executeQuery()) {
			if (rs.next())
				return true;
		}
		return false;
	}

	public static boolean alreadyProcessed(Map<String, Boolean> seen, AuthorityParsedData toCheck) {
		String key = toCheck.id + toCheck.moddate + toCheck.numUpdates;
		if (seen.containsKey(key)) {
			return true;
		} else {
			seen.put(key,  true);
			return false;
		}
	}

	public static int countNumUpdate(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = Utils.getIdAsList(adminMds);
		return adminMdIds.size();
	}

	public static AuthoritySource extractVocab(String id) {
		if (id.startsWith("gf")) return AuthoritySource.LCGFT;
		if (id.startsWith("sj")) return AuthoritySource.LCJSH;
		if (id.startsWith("sh")) return AuthoritySource.LCSH;
		if (id.startsWith("n"))  return AuthoritySource.NAF;
		return AuthoritySource.UNK;
	}

	/*
	 * For madsrdf:authoritativeLabel
	 * 1. if it is null, return null (happens for deprecated item)
	 * 2. if it is a string, return it
	 * 3. if it is a list, return the first string representation
	 *    if string representation is not found on the list, return @value from last object representation
	 * 4. if it is an object (map), return the @value
	 * If above doesn't resolve to anything, return null
	 */
	public static String getAuthorativeLabel(JsonObject mainEntry) {
		JsonValue authLabel = mainEntry.get("madsrdf:authoritativeLabel");
		if (authLabel == null)
			return null;
		if (authLabel.getValueType() == ValueType.STRING)
			return Utils.getString(authLabel);
		String labelForLang = null;
		if (authLabel.getValueType() == ValueType.ARRAY) {
			for (JsonValue label : authLabel.asJsonArray()) {
				if (label.getValueType() == ValueType.STRING)
					return Utils.getString(label);
				else if (label.getValueType() == ValueType.OBJECT)
					labelForLang = label.asJsonObject().getJsonString("@value").getString();
			}
		} else if (authLabel.getValueType() == ValueType.OBJECT)
			return authLabel.asJsonObject().getString("@value");
		return labelForLang;
	}

	public static String getDestName(String url) throws URISyntaxException {
		URI uri = new URI(url);
		String path = uri.getPath();
		String basename = path.substring(path.lastIndexOf('/') + 1);
		if (!basename.endsWith(".gz"))
			throw new IllegalArgumentException("Input is not gzip! " + url);

		return basename.substring(0, basename.length() - 3);
	}

	public static List<String> getIdAsList(JsonValue arg) {
		List<String> ids = new ArrayList<>();
		if (arg == null) return ids;

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

	public static JsonObject getLatestRecordInfo(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = Utils.getIdAsList(adminMds);
		JsonObject recordInfo = JsonValue.EMPTY_JSON_OBJECT;
		String latest = null;
		for (String adminMdId : adminMdIds) {
			JsonObject ri = Utils.getJsonObjectForId(graph, adminMdId);
			if (latest == null) {
				recordInfo = ri;
				latest = recordInfo.getJsonObject("ri:recordChangeDate").getJsonString("@value").getString();
			} else {
				String changeDate = ri.getJsonObject("ri:recordChangeDate").getJsonString("@value").getString();
				if (changeDate.compareToIgnoreCase(latest) > 0) {
					recordInfo = ri;
					latest = changeDate;
				}
			}
		}
		return recordInfo;
	}

	public static List<JsonObject> getRwos(JsonObject node, JsonArray graph) {
		List<JsonObject> rwos = new ArrayList<>();
		JsonValue rwosVal = node.get("madsrdf:identifiesRWO");
		List<String> rwoIds = getIdAsList(rwosVal);
		for (String rwoId : rwoIds) {
			JsonObject rwo = getJsonObjectForId(graph, rwoId);
			if (rwo.isEmpty()) continue;

			rwos.add(rwo);
		}
		return rwos;
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

		return getString(value);
	}

	/*
	 * Shorthand to get string value from a generic type JsonValue
	 * If it is an object, return the @value as string
	 * If it is an array, return the first element as string
	 */
	public static String getString(JsonValue value) {
		if (value.getValueType() == ValueType.STRING)
			return ((JsonString) value).getString();
		else if (value.getValueType() == ValueType.OBJECT)
			return value.asJsonObject().getJsonString("@value").getString();
		else if (value.getValueType() == ValueType.ARRAY)
			return getString(value.asJsonArray().get(0));
		return null;
	}

	public static String getToday() {
		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return now.format(formatter);
	}

	public static List<String> getListForArray(JsonObject node, String key) {
		List<String> list = new ArrayList<>();
		JsonValue value = node.get(key);
		if (value == null)
			return list;
		if (value.getValueType() == ValueType.ARRAY) {
			for (JsonValue val : value.asJsonArray()) {
				if (val.getValueType() == ValueType.OBJECT)
					list.add(val.asJsonObject().getJsonString("@id").getString());
				else
					list.add(((JsonString) val).getString());
			}
		} else if (value.getValueType() == ValueType.OBJECT)
			list.add(value.asJsonObject().getJsonString("@id").getString());
		else if (value.getValueType() == ValueType.STRING)
			list.add(((JsonString) value).getString());
		return list;
	}

	public static <T> HttpResponse<T> httpGet(String url, BodyHandler<T> bodyHandler) throws IOException, InterruptedException {
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(url)).build();
		return HTTP_CLIENT.send(request, bodyHandler);
	}

	public static boolean parseIsMemberOfMADSCollection(JsonValue isMemberOf, String idUrl) {
		if (isMemberOf == null)
			return false;
		if (isMemberOf.getValueType() == ValueType.STRING)
			return idUrl.equalsIgnoreCase(Utils.getString(isMemberOf));
		else if (isMemberOf.getValueType() == ValueType.OBJECT)
			return idUrl.equalsIgnoreCase(Utils.getString(isMemberOf.asJsonObject(), "@id"));
		else if (isMemberOf.getValueType() == ValueType.ARRAY) {
			for (JsonValue memberOf : isMemberOf.asJsonArray()) {
				if (idUrl.equalsIgnoreCase(memberOf.asJsonObject().getJsonString("@id").getString()))
					return true;
			}
		}
		return false;
	}

	public static boolean parseIsSubdivision(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.SUBDIV_URL);
	}

	public static JsonObject parseJsonLd(InputStream is) throws JsonLdError {
		Document document = JsonDocument.of(is);
		Optional<JsonStructure> jsonContentOptional = document.getJsonContent();
		if (jsonContentOptional.isPresent()) {
			JsonStructure jsonContent = jsonContentOptional.get();
			if (jsonContent.getValueType() == ValueType.OBJECT)
				return jsonContent.asJsonObject();
		}

		throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Failed to parse jsonld");
	}

	public static AuthorityParsedData parseAuthorityData(JsonObject doc) throws JsonLdError, URISyntaxException, IOException {
		return parseAuthorityData(doc, null);
	}

	public static AuthorityParsedData parseAuthorityData(JsonObject doc, String docUri) throws JsonLdError, URISyntaxException, IOException {
		JsonArray graph = doc.getJsonArray("@graph");
		if (docUri == null) {
			docUri = "http://id.loc.gov";
			var val = doc.getJsonString("@id");
			// bulk download format has @id at the root level
			if (val != null)
				docUri += doc.getJsonString("@id").getString();
			else
				throw new JsonLdError(JsonLdErrorCode.UNSPECIFIED, "ID not found for bulk download entry");
		} else
			docUri = docUri.replace("https://", "http://");

		AuthorityParsedData parsed = new AuthorityParsedData();
		JsonObject mainEntry = Utils.getJsonObjectForId(graph, docUri);

		parsed.id = mainEntry.getJsonString("@id").getString();
		parsed.lccn = Utils.getString(mainEntry, "identifiers:lccn");

		String path = new URI(parsed.id).getPath();
		int lastSlashIndex = path.lastIndexOf('/');
		parsed.vocab = extractVocab(path.substring(lastSlashIndex + 1));

		JsonValue isMemberOf = mainEntry.get("madsrdf:isMemberOfMADSCollection");
		parsed.undifferentiated = parseUndifferentiated(isMemberOf);
		parsed.isSubdivision = parseIsSubdivision(isMemberOf);

		parsed.authorativeLabel = getAuthorativeLabel(mainEntry);
		// If subdivision, we will try to replace -- to >
		if (parsed.isSubdivision)
			parsed.authorativeLabel = parsed.authorativeLabel.replace("--", " > ");

		parsed.headingType = headingType(mainEntry, docUri);

		JsonObject riRecord = getLatestRecordInfo(graph, mainEntry);
		parsed.moddate = getString(riRecord, "ri:recordChangeDate");

		String recordStatus = getString(riRecord, "ri:recordStatus");
		parsed.recordStatus = MadsRecordStatus.byName(recordStatus);

		parsed.numUpdates = countNumUpdate(graph, mainEntry);

		try (StringWriter sw = new StringWriter();
			 JsonWriter jsonWriter = Json.createWriter(sw)) {
			jsonWriter.write(doc);
			parsed.source = sw.toString();
		}

		return parsed;
	}

	public static MadsHeadingType headingType(JsonObject mainEntry, String docUri) {
		List<MadsHeadingType> types = parseHeadingType(mainEntry);
		switch (types.size()) {
		case 0:
			System.out.println("Identified no Mads heading type for " + docUri);
			return null;
		case 1:
			return types.get(0);
		default:
			System.out.println("Identified multiple Mads heading types for " + docUri + " : " + types);
			return types.get(0);
		}
	}

	public static AuthorityParsedData parseAuthorityData(String jsonld) throws JsonLdError, URISyntaxException, IOException {
		JsonObject doc = parseJsonLd(jsonld);
		return parseAuthorityData(doc);
	}

	public static boolean parseUndifferentiated(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.UNDIFF_URL);
	}

	public static List<MadsHeadingType> parseHeadingType(JsonObject mainEntry) {
		JsonValue ht = mainEntry.get("@type");
		List<MadsHeadingType> types = new ArrayList<>();
		if (ht.getValueType() == ValueType.ARRAY) {
			for (JsonValue type : ht.asJsonArray()) {
				MadsHeadingType t = MadsHeadingType.byType(Utils.getString(type));
				if (t != null)
					types.add(t);
			}
		} else if (ht.getValueType() == ValueType.STRING) {
			MadsHeadingType t = MadsHeadingType.byType(Utils.getString(ht));
			if (t != null)
				types.add(t);
		}
		return types;
	}

	public static JsonObject parseJsonLd(String jsonld) throws JsonLdError {
		StringReader reader = new StringReader(jsonld);
		Document document = JsonDocument.of(reader);
		Optional<JsonStructure> jsonContentOptional = document.getJsonContent();
		if (jsonContentOptional.isPresent()) {
			JsonStructure jsonContent = jsonContentOptional.get();
			if (jsonContent.getValueType() == ValueType.OBJECT)
				return jsonContent.asJsonObject();
		}

		throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Failed to parse jsonld " + jsonld);
	}

	public static PreparedStatement replaceStmt(Connection authorityDB) throws SQLException {
		return authorityDB.prepareStatement(
				"""
REPLACE INTO %s (id,lccn,vocabulary,recordStatus,heading,headingType,isSubdivision,undifferentiated,moddate,addedDate,numUpdates,source)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""".formatted(UPDATE_TABLE));
	}

	public static PreparedStatement selectMostRecentStmt(Connection authorityDB, String id) throws SQLException {
		return authorityDB.prepareStatement("SELECT * FROM %s WHERE id = ? order by addedDate desc LIMIT 1".formatted(UPDATE_TABLE));
	}

	public static AuthorityMap mostRecentMadsRecord(Connection authority, String identifier) throws SQLException, JsonLdError, IOException {
		try (PreparedStatement stmt = Utils.selectMostRecentStmt(authority, identifier)) {
			stmt.setString(1, identifier);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					return AuthorityMap.fromMadsJsonld(authority, rs.getString("id"), rs.getString("source"), rs.getBoolean("undifferentiated"));
//					try (ByteArrayInputStream is = new ByteArrayInputStream(rs.getString(1).getBytes(StandardCharsets.UTF_8))) {
//						return JsonDocument.of(is);
//					}
				}
			}
		}
		return null;
	}

	public static String maxAddedDate(Connection authority) throws SQLException {
		try (PreparedStatement pstmt = authority.prepareStatement("SELECT MAX(addedDate) as maxAddedDate FROM %s".formatted(Utils.UPDATE_TABLE));
			 ResultSet rs = pstmt.executeQuery()) {
			if (rs.next()) return rs.getString(1);
		}

		throw new SQLException("No addedDate in system!");
	}

	public static Set<String> allMadsIdentifiers(Connection authority) throws SQLException {
		Set<String> identifiers = new TreeSet<>();
		try (Statement stmt = authority.createStatement()) {
			 try (ResultSet rs = stmt.executeQuery("SELECT DISTINCT id FROM %s".formatted(Utils.UPDATE_TABLE))) {
////			String sql = "SELECT DISTINCT id FROM %s WHERE id = 'http://id.loc.gov/authorities/names/n90633346'".formatted(Utils.UPDATE_TABLE);			
//			System.out.println(sql);
//			try (ResultSet rs = stmt.executeQuery(sql)) {
				while (rs.next()) identifiers.add(rs.getString(1));
			}
			System.out.format("%d distinct records in %s.\n".formatted(identifiers.size(), Utils.UPDATE_TABLE));
		}
//		identifiers.add("");
//		identifiers.add("http://id.loc.gov/authorities/names/n90633346");
//		identifiers.add("http://id.loc.gov/authorities/names/n83133203");
		return identifiers;
	}

	public static JsonObject madsDoc(Connection authority, String identifier) throws SQLException, IOException, JsonLdError {
		try (PreparedStatement pstmt = authority.prepareStatement("SELECT source FROM %s WHERE id = ?".formatted(Utils.UPDATE_TABLE))) {
			pstmt.setString(1, identifier);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (!rs.next()) return null;

				return parseJsonLd(rs.getString(1));
//				try (ByteArrayInputStream is = new ByteArrayInputStream(rs.getString(1).getBytes(StandardCharsets.UTF_8))) {
//					Document doc = JsonDocument.of(is);
//					return 
//				}
			}
		}
	}

	public static Set<String> madsIdentifiersNewerThan(Connection authorityDB, String cursor) throws SQLException {
		Set<String> identifiers = new TreeSet<>();
		try ( PreparedStatement pstmt = authorityDB.prepareStatement("SELECT DISTINCT id FROM %s WHERE addedDate > ?".formatted(UPDATE_TABLE))) {
			pstmt.setString(1, cursor);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next())
					identifiers.add(rs.getString(1));
			}
		}
		return identifiers;
	}

	public static AuthorityParsedData headingFromLcId(Connection authority, String id) throws SQLException {
		try ( PreparedStatement pstmt = authority.prepareStatement("SELECT heading, headingType FROM %s WHERE id = ? ORDER BY addedDate desc LIMIT 1".formatted(UPDATE_TABLE))) {
			pstmt.setString(1, id);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					AuthorityParsedData rec = new AuthorityParsedData();
					rec.authorativeLabel = rs.getString("heading");
					rec.headingType = MadsHeadingType.byOrdinal(rs.getInt("headingType"));
					return rec;
				}
			}
		}
		return null;
	}

	public static void setUpDatabase(Connection authority) throws SQLException {
		List<String> sqls = Arrays.asList(
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
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci""".formatted(UPDATE_TABLE),
				"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` TINYINT(3) unsigned NOT NULL,
	`name` VARCHAR(256) NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=MyISAM""".formatted(HEADING_TYPE_TABLE),
				"""
CREATE TABLE IF NOT EXISTS `%s` (
	`id` TINYINT(1) unsigned NOT NULL,
	`name` VARCHAR(256) NOT NULL,
	PRIMARY KEY (`id`)
) ENGINE=MyISAM""".formatted(RECORD_STATUS_TABLE)
);

		try ( Statement stmt = authority.createStatement() ) {
			for (String sql : sqls)
				stmt.execute(sql);
		}

		try ( PreparedStatement insertDesc = authority.prepareStatement(
				"REPLACE INTO %s (id,name) VALUES (? , ?)".formatted(HEADING_TYPE_TABLE)) ) {
		for ( MadsHeadingType ht : MadsHeadingType.values()) {
			insertDesc.setInt(1, ht.ordinal());
			insertDesc.setString(2, ht.toString());
			insertDesc.executeUpdate();
		}}

		try ( PreparedStatement insertDesc = authority.prepareStatement(
				"REPLACE INTO %s (id,name) VALUES (? , ?)".formatted(RECORD_STATUS_TABLE)) ) {
		for ( MadsRecordStatus rs : MadsRecordStatus.values()) {
			insertDesc.setInt(1, rs.ordinal());
			insertDesc.setString(2, rs.toString());
			insertDesc.executeUpdate();
		}}
	}

	public static void printJsonLd(JsonObject doc) throws IOException {
		try (StringWriter sw = new StringWriter();
			 JsonWriter jsonWriter = Json.createWriter(sw)) {
			jsonWriter.write(doc);
			System.out.println(sw.toString());
		}
	}
}
