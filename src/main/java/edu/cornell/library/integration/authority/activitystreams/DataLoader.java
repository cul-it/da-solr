package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.mads.AuthorityData;
import edu.cornell.library.integration.authority.mads.JsonldUtils;
import edu.cornell.library.integration.utilities.Config;
import jakarta.json.JsonObject;

public class DataLoader {
	public static void main(String[] args) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Authority");
		Config config = Config.loadConfig(requiredArgs);
		Map<String, String> env = System.getenv();
		int chunkSize = Integer.parseInt(env.getOrDefault("ChunkSize", "100"));
		String idList = env.getOrDefault("idList", "/cul/app/scratch/data/id_list.txt");
		String addedDate = Utils.getToday();
		String dataset = env.getOrDefault("dataset", "LCNAF");
		var params = Dataset.getParam(dataset);
		if (params == null) {
			System.out.println("Unknown dataset provided: " + dataset);
			System.exit(1);
		}

		System.out.println("Chunk size: " + chunkSize);
		IFetcher fetcher = new IFetcher.HttpFetcher();
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			DataLoader loader = new DataLoader();
			loader.processData(addedDate, authority, fetcher, idList, params.contextUrl);
		}
		System.out.println("Complete!");
	}

	protected List<String> getAllIdentifiers(String idList) throws IOException {
		Path path = Paths.get(idList);
		return Files.readAllLines(path);
	}

	public void processData(String addedDate, Connection authority, IFetcher fetcher, String idList, String contextUrl) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		try (PreparedStatement insertStmt = Utils.replaceStmt(authority);
			 PreparedStatement existsStmt = Utils.existsStmt(authority)) {
			List<String> urls = getAllIdentifiers(idList);
			for (String url : urls) {
				AuthorityData parsed = fetchAndParse(fetcher, url, contextUrl);
				if (!Utils.exists(existsStmt, parsed))
					Utils.addBatch(insertStmt, parsed, addedDate);
			}
			insertStmt.executeBatch();
		}
	}

	public AuthorityData fetchAndParse(IFetcher fetcher, String url, String context) throws InterruptedException, IOException, JsonLdError, URISyntaxException {
		try (InputStream is = fetcher.fetch(url + ".madsrdf.json")) {
			Document doc = JsonDocument.of(is);
			JsonObject compact = JsonLd.compact(doc, context).get();
			return JsonldUtils.parseAuthorityData(compact, url);
		}
	}
}
