package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface IFetcher {
	public static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().followRedirects(Redirect.NORMAL).build();

	public InputStream fetch(String url) throws IOException, InterruptedException;
	
	class HttpFetcher implements IFetcher {
		@Override
		public InputStream fetch(String url) throws IOException, InterruptedException {
			url = url.replace("http://", "https://");
			HttpResponse<InputStream> resp = httpGet(url, HttpResponse.BodyHandlers.ofInputStream());
			return resp.body();
		}

		public static <T> HttpResponse<T> httpGet(String url, BodyHandler<T> bodyHandler) throws IOException, InterruptedException {
			HttpRequest request = HttpRequest.newBuilder()
					.uri(URI.create(url)).build();
			return HTTP_CLIENT.send(request, bodyHandler);
		}
	}

	class FileFetcher implements IFetcher {
		String localDir = null;
		boolean debug = false;

		public FileFetcher(Path localDir) {
			this.localDir = localDir.toAbsolutePath().toString();
		}

		public FileFetcher(Path localDir, boolean debug) {
			this.localDir = localDir.toAbsolutePath().toString();
			this.debug = debug;
		}

		@Override
		public InputStream fetch(String url) throws IOException, InterruptedException {
			String basename = url.substring(url.lastIndexOf('/') + 1);
			Path path = Path.of(localDir, basename);
			if (! Files.exists(path)) {
				path = Path.of(localDir, "rwo", basename);
			}
			if (debug) {
				System.out.println("Fetching from local file: " + path);
				if (! Files.exists(path)) {
					System.out.println("File does not exist: " + path);
				}
			}
			return Files.newInputStream(path, StandardOpenOption.READ);
		}
	}
}
