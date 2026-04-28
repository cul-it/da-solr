package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;

public interface IFetcher {
	public InputStream fetch(String url) throws IOException, InterruptedException;
}
