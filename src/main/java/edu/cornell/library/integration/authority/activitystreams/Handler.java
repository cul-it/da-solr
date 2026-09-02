package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.activitystreams.ActivityStreams.OrderedItem;
import edu.cornell.library.integration.authority.mads.AuthorityData;
import edu.cornell.library.integration.authority.mads.JsonldUtils;
import edu.cornell.library.integration.utilities.Config;

public class Handler {
	IHandlerConfig handlerConfig;

	public static void main(String[] args) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		Map<String, String> env = System.getenv();
		int chunkSize = Integer.parseInt(env.getOrDefault("ChunkSize", "100"));
		String dataset = env.get("dataset");
		String addedDate = Utils.getToday();
		IHandlerConfig handlerConfig = new IHandlerConfig.HandlerConfig(addedDate, chunkSize, dataset);

		System.out.println("Added date: " + addedDate);
		System.out.println("Dataset: " + dataset);
		System.out.println("Chunk size: " + chunkSize);

		try (Connection authority = config.getDatabaseConnection("Authority")) {
			Handler handler = new Handler(handlerConfig);
			handler.processData(authority);
		}
		System.out.println("Complete!");
	}

	public Handler(IHandlerConfig handlerConfig) {
		this.handlerConfig = handlerConfig;
	}

	public void processData(Connection authority) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		var params = handlerConfig.datasetEntry();
		String url = params.url;
		var fetcher = handlerConfig.fetcher();
		try (PreparedStatement insertStmt = Utils.replaceStmt(authority);
			 PreparedStatement existsStmt = Utils.existsStmt(authority)) {
			Map<String, Boolean> seen = new HashMap<>();
			do {
				try (InputStream is = fetcher.fetch(url)) {
					var activityStreams = parseActivityStreams(is);
					for (OrderedItem orderedItem : activityStreams.orderedItems) {
						var parsed = fetchAndParse(fetcher, orderedItem.id, params.contextUrl);
						if (Utils.alreadyProcessed(seen, parsed))
							continue;

						if (Utils.exists(existsStmt, parsed)) {
							activityStreams.next = null;
							System.out.println(String.format("Existing record found, stopping... %s, %s, %s", parsed.id, parsed.numUpdates, parsed.moddate));
							break;
						} else
							Utils.addBatch(insertStmt, parsed, handlerConfig.addedDate());
					}
					var inserted = insertStmt.executeBatch();
					if (inserted.length > 0)
						System.out.println("Processed " + url);
					url = activityStreams.next;
				}
			} while (url != null);
		}
	}

	public AuthorityData fetchAndParse(IFetcher fetcher, String url, String context) throws InterruptedException, IOException, JsonLdError, URISyntaxException {
		try (InputStream is = fetcher.fetch(url + ".madsrdf.json")) {
			var doc = JsonDocument.of(is);
			var compact = JsonLd.compact(doc, context).get();
			return JsonldUtils.parseAuthorityData(compact, url);
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
		var doc = JsonldUtils.parseJsonLd(is);
		activityStreams.id = JsonldUtils.getString(doc, "id");
		activityStreams.next = JsonldUtils.getString(doc, "next");
		for (var val : doc.getJsonArray("orderedItems")) {
			var obj = val.asJsonObject().getJsonObject("object");
			var url = obj.getJsonArray("url");
			activityStreams.addOrderedItem(JsonldUtils.getString(obj, "id"), ActivityStreams.resolveLinkUrl(url));
		}
		return activityStreams;
	}
}
