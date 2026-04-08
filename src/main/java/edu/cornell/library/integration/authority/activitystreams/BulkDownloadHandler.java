package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.AuthoritySource;
import edu.cornell.library.integration.utilities.Config;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;

public class BulkDownloadHandler {
	public static void main(String[] args) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		Map<String, String> env = System.getenv();
		int chunkSize = Integer.parseInt(env.getOrDefault("ChunkSize", "100"));
		boolean deleteOldFile = Boolean.parseBoolean(env.getOrDefault("DeleteOldFile", "false"));
		boolean deleteTempFileOnCompletion = Boolean.parseBoolean(env.getOrDefault("DeleteTempFileOnCompletion", "true"));
		String destinationDir = env.get("DestinationDir");
		boolean initDB = Boolean.parseBoolean(env.getOrDefault("initDB", "false"));
		String jsonldURL = env.get("BulkDownloadURL");
		String addedDate = Helper.getToday();

		try (Connection authority = config.getDatabaseConnection("Authority")) {
			Path destination = Paths.get(destinationDir, Helper.getDestName(jsonldURL));
			System.out.println("Chunk size: " + chunkSize);
			System.out.println("DeleteOldFile: " + deleteOldFile);
			System.out.println("Destination: " + destination);
			System.out.println("init DB: " + initDB);
			System.out.println("jsonldURL: " + jsonldURL);
			System.out.println("Added date: " + addedDate);
			System.out.println("Bulk download handler started...");
			BulkDownloadHandler handler = new BulkDownloadHandler();
			handler.run(addedDate, authority, chunkSize, deleteOldFile, destination, initDB, jsonldURL);
			System.out.println("Complete!");
			if (deleteTempFileOnCompletion) {
				System.out.print("Removing temporary file... ");
				Files.delete(destination);
				System.out.println("Done!");
			}
		}
	}

	public void run(String addedDate, Connection authority, int chunkSize, boolean deleteOldFile, Path destination, boolean initDB, String jsonldURL) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		if (initDB)
			Helper.setUpDatabase(authority);

		BulkDownloadHandler handler = new BulkDownloadHandler();

		if (deleteOldFile)
			Files.delete(destination);

		if (! Files.exists(destination))
			handler.downloadBulkJsonLd(jsonldURL, destination);

		handler.processData(addedDate, destination, authority, chunkSize);
	}

	public void downloadBulkJsonLd(String url, Path destination) throws IOException, InterruptedException {
		HttpClient client = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NORMAL)
				.build();
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(url)).build();
		HttpResponse<InputStream> response = client.send(
				request, HttpResponse.BodyHandlers.ofInputStream());
		if (response.statusCode() != 200) {
			System.out.println("Failed to bulk download file at " + url);
			System.out.println("Response status code: " + response.statusCode());
			System.out.println(response.toString());
			System.exit(1);
		}

		try (InputStream gzipStream = new GZIPInputStream(response.body());
			 OutputStream outputStream = Files.newOutputStream(destination, StandardOpenOption.CREATE)) {
			byte[] buffer = new byte[8192];
			int length;
			while ((length = gzipStream.read(buffer)) > 0)
				outputStream.write(buffer, 0, length);
		}
	}

	public void processData(String addedDate, Path bulkFile, Connection authorityDB, int chunkSize) throws IOException, JsonLdError, SQLException, URISyntaxException {
		/*
		 * Java 22 has Preview feature for Stream Gatherers
		 * Stream<List<Integer>> chunkedStream = Stream.of(1, 2, 3, 4, 5)
		 * 		.gather(Gatherers.windowFixed(3)); // [[1, 2, 3], [4, 5]]
		 * We may refactor this code to use that in later releases.
		 */
		try (Stream<String> lines = Files.lines(bulkFile);
			 PreparedStatement insertStmt = Helper.replaceStmt(authorityDB)) {
			Iterator<String> it = lines.iterator();
			while (it.hasNext()) {
				for (int i = 0; i < chunkSize && it.hasNext(); i++) {
					String line = it.next();
					AuthorityParsedData data = parseBulkEntry(line);
					Helper.addBatch(insertStmt, data, addedDate, line);
				}
				insertStmt.executeBatch();
			}
		}
	}

	public AuthorityParsedData parseBulkEntry(String jsonld) throws JsonLdError, URISyntaxException {
		JsonObject doc = parseJsonLd(jsonld);
		String docUri = "http://id.loc.gov" + doc.getJsonString("@id").getString();
		JsonArray graph = doc.getJsonArray("@graph");
		
		AuthorityParsedData parsed = new AuthorityParsedData();
		JsonObject mainEntry = Helper.getJsonObjectForId(graph, docUri);

		parsed.id = mainEntry.getJsonString("@id").getString();
		parsed.lccn = Helper.getString(mainEntry, "identifiers:lccn");

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
		parsed.recordStatus = MadsRecordStatus.byName(recordStatus);

		parsed.numUpdates = countNumUpdate(graph, mainEntry);

		return parsed;
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

		throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Failed to parse jsonld " + jsonld);
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
			return Helper.getString(authLabel);
		String labelForLang = null;
		if (authLabel.getValueType() == ValueType.ARRAY) {
			for (JsonValue label : authLabel.asJsonArray()) {
				if (label.getValueType() == ValueType.STRING)
					return Helper.getString(label);
				else if (label.getValueType() == ValueType.OBJECT)
					labelForLang = label.asJsonObject().getJsonString("@value").getString();
			}
		} else if (authLabel.getValueType() == ValueType.OBJECT)
			return authLabel.asJsonObject().getString("@value");
		return labelForLang;
	}

	public MadsHeadingType parseHeadingType(JsonObject mainEntry) {
		JsonValue ht = mainEntry.get("@type");
		if (ht.getValueType() == ValueType.ARRAY) {
			for (JsonValue type : ht.asJsonArray()) {
				MadsHeadingType t = MadsHeadingType.byType(Helper.getString(type));
				if (t != null)
					return t;
			}
		} else if (ht.getValueType() == ValueType.STRING)
			return MadsHeadingType.byType(Helper.getString(ht));
		return null;
	}

	public JsonObject getLatestRecordInfo(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = Helper.getIdAsList(adminMds);
		JsonObject recordInfo = JsonValue.EMPTY_JSON_OBJECT;
		String latest = null;
		for (String adminMdId : adminMdIds) {
			JsonObject ri = Helper.getJsonObjectForId(graph, adminMdId);
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

	public int countNumUpdate(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = Helper.getIdAsList(adminMds);
		return adminMdIds.size();
	}

	protected boolean parseIsMemberOfMADSCollection(JsonValue isMemberOf, String idUrl) {
		if (isMemberOf == null)
			return false;
		if (isMemberOf.getValueType() == ValueType.STRING)
			return idUrl.equalsIgnoreCase(Helper.getString(isMemberOf));
		else if (isMemberOf.getValueType() == ValueType.OBJECT)
			return idUrl.equalsIgnoreCase(Helper.getString(isMemberOf.asJsonObject(), "@id"));
		else if (isMemberOf.getValueType() == ValueType.ARRAY) {
			for (JsonValue memberOf : isMemberOf.asJsonArray()) {
				if (idUrl.equalsIgnoreCase(memberOf.asJsonObject().getJsonString("@id").getString()))
					return true;
			}
		}
		return false;
	}

	public boolean parseIsSubdivision(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.SUBDIV_URL);
	}

	public boolean parseUndifferentiated(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.UNDIFF_URL);
	}

	protected AuthoritySource extractVocab(String id) {
		if (id.startsWith("gf")) return AuthoritySource.LCGFT;
		if (id.startsWith("sj")) return AuthoritySource.LCJSH;
		if (id.startsWith("sh")) return AuthoritySource.LCSH;
		if (id.startsWith("n"))  return AuthoritySource.NAF;
		return AuthoritySource.UNK;
	}
}
