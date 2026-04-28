package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpResponse;

public class HttpFetcher implements IFetcher {

	@Override
	public InputStream fetch(String url) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		HttpResponse<InputStream> resp = Utils.httpGet(url, HttpResponse.BodyHandlers.ofInputStream());
		return resp.body();
	}

}
