package edu.cornell.library.integration.authority;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;

public class AuthorityJsonLDHandler {
	public static final String SUBDIV_URL = "http://id.loc.gov/authorities/subjects/collection_Subdivisions";
	public static final String UNDIFF_URL = "http://id.loc.gov/authorities/names/collection_NamesUndifferentiated";

	public void loadBulkJsonLD(String url, Connection authorityDB) throws IOException, InterruptedException, SQLException {
		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(url)).build();
		HttpResponse<InputStream> response = client.send(
				request, HttpResponse.BodyHandlers.ofInputStream());

		InputStream bodyStream = response.body();
		storeRawData(bodyStream, authorityDB);
		processData(authorityDB);
	}

	public void storeRawData(InputStream jsonLdStream, Connection authorityDB) throws IOException, SQLException {
		int count = 0;
		try (   BufferedReader reader = new BufferedReader(new InputStreamReader(jsonLdStream, StandardCharsets.UTF_8));
				PreparedStatement insertStmt = authorityDB.prepareStatement(
						"REPLACE INTO authorityRawJsonLD (json) VALUES (?)")) {
			String line;
			while ((line = reader.readLine()) != null) {
				insertStmt.setString(1, line);
				insertStmt.addBatch();
				count++;
				if ( 0 == count % 100 ) insertStmt.executeBatch();
			}
			if ( 0 != count % 100 ) insertStmt.executeBatch();
		}
	}

	public void processData(Connection authorityDB) throws IOException, SQLException {
		int count = 0;
		try (   PreparedStatement stmt = authorityDB.prepareStatement("SELECT id, json FROM authorityUpdateJsonLD");
				PreparedStatement insertStmt = authorityDB.prepareStatement(
						"REPLACE INTO authorityUpdateJsonLD"+
						"       (id,vocabulary,recordStatus,"+
						"        heading,headingType,isSubdivision,"+
						"        undifferentiated,moddate,rawId) "+
						"VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
			try ( ResultSet rs = stmt.executeQuery()) {
				while ( rs.next() ) {
					int rawId = rs.getInt(0);
					String raw = rs.getString(1);
					try {
						AuthorityParsedData data = parseBulkEntry(raw);
						insertStmt.setString(1, data.id);
						insertStmt.setInt(2, data.vocab.ordinal());
						insertStmt.setInt(3, data.recordStatus.ordinal());
						insertStmt.setString(4, data.authorativeLabel);
						insertStmt.setInt(5, data.headingType.ordinal());
						insertStmt.setBoolean(6, data.isSubdivision);
						insertStmt.setBoolean(7, data.undifferentiated);
						insertStmt.setString(8, data.moddate);
						insertStmt.setInt(9, rawId);
						insertStmt.addBatch();

						if ( 0 == count % 100 ) insertStmt.executeBatch();
						count++;
					} catch (JsonLdError | URISyntaxException e) {
						e.printStackTrace();
					}
				}
			}
			if ( 0 != count % 100 ) insertStmt.executeBatch();
		}
	}

	public AuthorityParsedData parseBulkEntry(String jsonld) throws JsonLdError, URISyntaxException {
		JsonObject doc = parseJsonLd(jsonld);
		String docUri = "http://id.loc.gov" + doc.getJsonString("@id").getString();
		JsonArray graph = doc.getJsonArray("@graph");
		return parseAuthorityData(graph, docUri);
	}

	public JsonObject parseJsonLd(String jsonld) throws JsonLdError {
		StringReader reader = new StringReader(jsonld);
		Document document = JsonDocument.of(reader);
		Optional<JsonStructure> jsonContentOptional = document.getJsonContent();
		if (jsonContentOptional.isPresent()) {
			JsonStructure jsonContent = jsonContentOptional.get();
			if (jsonContent.getValueType() == ValueType.OBJECT)
				return jsonContent.asJsonObject();
		}

		throw new JsonLdError(null, "Failed to parse jsonld " + jsonld);
	}

	public AuthorityParsedData parseAuthorityData(JsonArray graph, String docUri) throws URISyntaxException {
		AuthorityParsedData parsed = new AuthorityParsedData();
		JsonObject mainEntry = getJsonObjectForId(graph, docUri);

		parsed.id = mainEntry.getJsonString("@id").getString();
		parsed.lccn = getString(mainEntry, "identifiers:lccn");

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

		parsed.headingType = parseHeadingType(mainEntry);

		JsonObject riRecord = getLatestRecordInfo(graph, mainEntry);
		parsed.moddate = riRecord.getJsonObject("ri:recordChangeDate").getString("@value");
		String recordStatus = riRecord.getString("ri:recordStatus");
		parsed.recordStatus = RecordStatus.byName(recordStatus);

		return parsed;
	}

	// Get string value or null if key doesn't exist or not a string type
	public String getString(JsonObject obj, String key) {
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
	public String getString(JsonValue value) {
		if (value.getValueType() == ValueType.STRING)
			return ((JsonString) value).getString();
		else if (value.getValueType() == ValueType.OBJECT)
			return value.asJsonObject().getJsonString("@value").getString();
		return null;
	}

	public JsonObject getJsonObjectForId(JsonArray graph, String id) {
		for (JsonValue entry : graph) {
			JsonObject obj = entry.asJsonObject();
			String entryId = obj.getString("@id");
			if (id.equalsIgnoreCase(entryId))
				return obj;
		}
		return JsonValue.EMPTY_JSON_OBJECT;
	}

	public List<String> getIdAsList(JsonValue arg) {
		List<String> ids = new ArrayList<>();
		if (arg.getValueType() == ValueType.ARRAY) {
			for (JsonValue val : arg.asJsonArray())
				ids.add(val.asJsonObject().getJsonString("@id").getString());
		} else if (arg.getValueType() == ValueType.OBJECT)
			ids.add(arg.asJsonObject().getJsonString("@id").getString());
		return ids;
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
	public String getAuthorativeLabel(JsonObject mainEntry) {
		JsonValue authLabel = mainEntry.get("madsrdf:authoritativeLabel");
		if (authLabel == null)
			return null;
		if (authLabel.getValueType() == ValueType.STRING)
			return getString(authLabel);
		String labelForLang = null;
		if (authLabel.getValueType() == ValueType.ARRAY) {
			for (JsonValue label : authLabel.asJsonArray()) {
				if (label.getValueType() == ValueType.STRING)
					return getString(label);
				else if (label.getValueType() == ValueType.OBJECT)
					labelForLang = label.asJsonObject().getJsonString("@value").getString();
			}
		} else if (authLabel.getValueType() == ValueType.OBJECT)
			return authLabel.asJsonObject().getString("@value");
		return labelForLang;
	}

	public HeadingTypeJsonLD parseHeadingType(JsonObject mainEntry) {
		JsonValue ht = mainEntry.get("@type");
		if (ht.getValueType() == ValueType.ARRAY) {
			for (JsonValue type : ht.asJsonArray()) {
				HeadingTypeJsonLD t = HeadingTypeJsonLD.byType(getString(type));
				if (t != null)
					return t;
			}
		} else if (ht.getValueType() == ValueType.STRING)
			return HeadingTypeJsonLD.byType(getString(ht));
		return null;
	}

	public JsonObject getLatestRecordInfo(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = getIdAsList(adminMds);
		JsonObject recordInfo = JsonValue.EMPTY_JSON_OBJECT;
		String latest = null;
		for (String adminMdId : adminMdIds) {
			JsonObject ri = getJsonObjectForId(graph, adminMdId);
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

	protected boolean parseIsMemberOfMADSCollection(JsonValue isMemberOf, String idUrl) {
		if (isMemberOf == null)
			return false;
		if (isMemberOf.getValueType() == ValueType.OBJECT)
			return idUrl.equalsIgnoreCase(getString(isMemberOf));
		else if (isMemberOf.getValueType() == ValueType.ARRAY) {
			for (JsonValue memberOf : isMemberOf.asJsonArray()) {
				if (idUrl.equalsIgnoreCase(memberOf.asJsonObject().getJsonString("@id").getString()))
					return true;
			}
		}

		return false;
	}

	public boolean parseIsSubdivision(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, SUBDIV_URL);
	}

	public boolean parseUndifferentiated(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, UNDIFF_URL);
	}

	protected AuthoritySource extractVocab(String id) {
		if (id.startsWith("gf")) return AuthoritySource.LCGFT;
		if (id.startsWith("sj")) return AuthoritySource.LCJSH;
		if (id.startsWith("sh")) return AuthoritySource.LCSH;
		if (id.startsWith("n"))  return AuthoritySource.NAF;
		return AuthoritySource.UNK;
	}
}
