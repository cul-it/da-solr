package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileFetcher implements IFetcher {
	String localDir = null;
	public FileFetcher(Path localDir) {
		this.localDir = localDir.toAbsolutePath().toString();
	}

	@Override
	public InputStream fetch(String url) throws IOException, InterruptedException {
		String basename = url.substring(url.lastIndexOf('/') + 1);
		Path path = Path.of(localDir, basename);
		return Files.newInputStream(path, StandardOpenOption.READ);
	}
}
