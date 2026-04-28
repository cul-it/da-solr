package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.activitystreams.ActivityStreams.OrderedItem;
import edu.cornell.library.integration.utilities.Config;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;

public class ActivityStreamsHandler {
	public static void main(String[] args) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		Map<String, String> env = System.getenv();
		int chunkSize = Integer.parseInt(env.getOrDefault("ChunkSize", "100"));
		String context = env.get("Context");
		String activityStreamsURL = env.get("ActivityStreamsURL");
		String addedDate = Utils.getToday();

		try (Connection authority = config.getDatabaseConnection("Authority")) {
			ActivityStreamsHandler handler = new ActivityStreamsHandler();
			handler.run(activityStreamsURL, addedDate, authority, chunkSize, context);
		}
	}

	public void run(String activityStreamsURL, String addedDate, Connection authority, int chunkSize, String context) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		System.out.println("Chunk size: " + chunkSize);
		System.out.println("jsonldURL: " + activityStreamsURL);

		IFetcher fetcher = new HttpFetcher();
		processData(activityStreamsURL, addedDate, authority, context, fetcher);
	}

	public void processData(String activityStreamsURL, String addedDate, Connection authority, String context, IFetcher fetcher) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		try (PreparedStatement insertStmt = Utils.replaceStmt(authority);
			 PreparedStatement existsStmt = Utils.existsStmt(authority)) {
			do {
				try (InputStream is = fetcher.fetch(activityStreamsURL)) {
					ActivityStreams activityStreams = parseActivityStreams(is);
					for (OrderedItem orderedItem : activityStreams.orderedItems) {
						AuthorityParsedData parsed = fetchAndParse(fetcher, orderedItem.id, context);
						if (Utils.exists(existsStmt, parsed)) {
							activityStreams.next = null;
							break;
						} else
							Utils.addBatch(insertStmt, parsed, addedDate);
					}
					insertStmt.executeBatch();
					activityStreamsURL = activityStreams.next;
				}
			} while (activityStreamsURL != null);
		}
	}

	public AuthorityParsedData fetchAndParse(IFetcher fetcher, String url, String context) throws InterruptedException, IOException, JsonLdError, URISyntaxException {
		try (InputStream is = fetcher.fetch(url)) {
			Document doc = JsonDocument.of(is);
			JsonObject compact = JsonLd.compact(doc, context).get();
			return Utils.parseAuthorityData(compact);
		}
	}

	protected ActivityStreams parseActivityStreams(InputStream is) throws IOException, InterruptedException, JsonLdError {
		/*
		 * "object": {
		 *   "id": "http://id.loc.gov/authorities/subjects/sh2025001856",
		 *   "url": [{},...
		 *   ...
		 * }
		 */
		ActivityStreams activityStreams = new ActivityStreams();
		JsonObject doc = Utils.parseJsonLd(is);
		activityStreams.id = Utils.getString(doc, "id");
		activityStreams.next = Utils.getString(doc, "next");
		for (JsonValue val : doc.getJsonArray("orderedItems")) {
			JsonObject obj = val.asJsonObject().getJsonObject("object");
			JsonArray url = obj.getJsonArray("url");
			activityStreams.addOrderedItem(Utils.getString(obj, "id"), ActivityStreams.resolveLinkUrl(url));
		}
		return activityStreams;
	}
}
