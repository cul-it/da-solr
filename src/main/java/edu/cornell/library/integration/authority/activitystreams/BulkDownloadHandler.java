package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
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
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.utilities.Config;

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
		int numChunksPerReport = Integer.parseInt(env.getOrDefault("NumChunksPerReport", "0"));
		String addedDate = Utils.getToday();

		try (Connection authority = config.getDatabaseConnection("Authority")) {
			Path destination = Paths.get(destinationDir, Utils.getDestName(jsonldURL));
			System.out.println("Chunk size: " + chunkSize);
			System.out.println("DeleteOldFile: " + deleteOldFile);
			System.out.println("Destination: " + destination);
			System.out.println("init DB: " + initDB);
			System.out.println("jsonldURL: " + jsonldURL);
			System.out.println("Added date: " + addedDate);
			System.out.println("Bulk download handler started...");
			BulkDownloadHandler handler = new BulkDownloadHandler();
			handler.run(addedDate, authority, chunkSize, deleteOldFile, destination, initDB, jsonldURL, numChunksPerReport);
			System.out.println("Complete!");
			if (deleteTempFileOnCompletion) {
				System.out.print("Removing temporary file...");
				Files.delete(destination);
				System.out.println("Done!");
			}
		}
	}

	public void run(String addedDate, Connection authority, int chunkSize, boolean deleteOldFile, Path destination, boolean initDB, String jsonldURL, int numChunksPerReport) throws InterruptedException, IOException, JsonLdError, SQLException, URISyntaxException {
		if (initDB)
			Utils.setUpDatabase(authority);

		BulkDownloadHandler handler = new BulkDownloadHandler();

		if (deleteOldFile)
			Files.delete(destination);

		if (! Files.exists(destination))
			handler.downloadBulkJsonLd(jsonldURL, destination);

		handler.processData(addedDate, destination, authority, chunkSize, numChunksPerReport);
	}

	public void downloadBulkJsonLd(String url, Path destination) throws IOException, InterruptedException {
		HttpResponse<InputStream> response = Utils.httpGet(url, HttpResponse.BodyHandlers.ofInputStream());
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

	public void processData(String addedDate, Path bulkFile, Connection authorityDB, int chunkSize, int numChunksPerReport) throws IOException, JsonLdError, SQLException, URISyntaxException {
		/*
		 * Java 22 has Preview feature for Stream Gatherers
		 * Stream<List<Integer>> chunkedStream = Stream.of(1, 2, 3, 4, 5)
		 * 		.gather(Gatherers.windowFixed(3)); // [[1, 2, 3], [4, 5]]
		 * We may refactor this code to use that in later releases.
		 */
		try (Stream<String> lines = Files.lines(bulkFile);
			 PreparedStatement insertStmt = Utils.replaceStmt(authorityDB)) {
			Iterator<String> it = lines.iterator();
			int processedChunks = 0;
			while (it.hasNext()) {
				for (int i = 0; i < chunkSize && it.hasNext(); i++) {
					String line = it.next();
					AuthorityParsedData data = Utils.parseAuthorityData(line);
					Utils.addBatch(insertStmt, data, addedDate);
				}
				insertStmt.executeBatch();
				if (numChunksPerReport > 0 && ++processedChunks % numChunksPerReport == 0)
					System.out.println("Processed " + String.format("%,d", processedChunks * chunkSize) + " entries");
			}
		}
	}
}
