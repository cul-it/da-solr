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
		String paramsName = env.get("paramsName");
		String addedDate = Utils.getToday();
		var params = ActivityStreamsParams.getParam(paramsName);
		if (params == null) {
			System.out.println("Unknown params name provided: " + paramsName);
			System.exit(1);
		}

		System.out.println("Chunk size: " + chunkSize);
		System.out.println("activity streams URL: " + params.url);
		System.out.println("context URL: " + params.contextUrl);
		IFetcher fetcher = new HttpFetcher();
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			ActivityStreamsHandler handler = new ActivityStreamsHandler();
			handler.processData(addedDate, authority, fetcher, params);
		}
	}

	public void processData(String addedDate, Connection authority, IFetcher fetcher, ActivityStreamsParamsEntry params) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		String url = params.url;
		try (PreparedStatement insertStmt = Utils.replaceStmt(authority);
			 PreparedStatement existsStmt = Utils.existsStmt(authority)) {
			do {
				try (InputStream is = fetcher.fetch(url)) {
					ActivityStreams activityStreams = parseActivityStreams(is);
					for (OrderedItem orderedItem : activityStreams.orderedItems) {
						AuthorityParsedData parsed = fetchAndParse(fetcher, orderedItem.id, params.contextUrl);
						if (Utils.exists(existsStmt, parsed)) {
							activityStreams.next = null;
							break;
						} else
							Utils.addBatch(insertStmt, parsed, addedDate);
					}
					insertStmt.executeBatch();
					url = activityStreams.next;
				}
			} while (url != null);
		}
	}

	public AuthorityParsedData fetchAndParse(IFetcher fetcher, String url, String context) throws InterruptedException, IOException, JsonLdError, URISyntaxException {
		System.out.println("Fetching " + url);
		try (InputStream is = fetcher.fetch(url + ".madsrdf.json")) {
			Document doc = JsonDocument.of(is);
			JsonObject compact = JsonLd.compact(doc, context).get();
			return Utils.parseAuthorityData(compact, url);
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
