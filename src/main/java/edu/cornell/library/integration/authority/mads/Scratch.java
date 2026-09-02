package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.activitystreams.IFetcher;

public class Scratch {
	public static void main(String[] args) throws IOException, InterruptedException, JsonLdError, URISyntaxException {
		String id = args[0];

		IFetcher fetcher = new IFetcher.HttpFetcher();
		try (InputStream is = fetcher.fetch(id + ".madsrdf.json")) {
			var doc = JsonldUtils.parseFlatJsonLd(is, id);
			var auth = JsonldUtils.parseAuthorityData(doc, id);
			auth.print();
		}
	}
}
