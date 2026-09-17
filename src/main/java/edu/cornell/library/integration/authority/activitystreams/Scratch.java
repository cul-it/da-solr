package edu.cornell.library.integration.authority.activitystreams;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.http.HttpResponse;
import java.util.Collections;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;

public class Scratch {
	static final String CONTEXT_URL = "https://id.loc.gov/authorities/names/context.json";
	static final String TEST_URL = "https://id.loc.gov/resources/instances/23837004.cbd.json";

	public static void main(String[] args) throws IOException, InterruptedException, JsonLdError {
		var resp = IFetcher.HttpFetcher.httpGet(TEST_URL, HttpResponse.BodyHandlers.ofInputStream());
		try (InputStream is = resp.body()) {
			Document doc = JsonDocument.of(is);
			JsonObject compact = JsonLd.compact(doc, CONTEXT_URL).get();
			JsonWriterFactory wf = Json.createWriterFactory(Collections.singletonMap(JsonGenerator.PRETTY_PRINTING, true));
			StringWriter sw = new StringWriter();
			try (JsonWriter jsonWriter = wf.createWriter(sw)) {
				jsonWriter.writeObject(compact);
				System.out.println(sw.toString());
			}
		}
	}
}
