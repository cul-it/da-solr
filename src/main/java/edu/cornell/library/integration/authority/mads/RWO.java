package edu.cornell.library.integration.authority.mads;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

public record RWO(String id, String label) {
	public void print(String padding) {
		// System.out.println(padding + "RWO: " + id + " - " + label);
	}

	public RWO fromJsonObject(JsonObject doc, String docUri) {
		JsonArray graph = doc.getJsonArray("@graph");
		JsonObject mainEntry = AuthorityJsonldUtils.getJsonObjectForId(graph, docUri);
		String rwoLabel = AuthorityJsonldUtils.getString(mainEntry, "rdfs:label");
		return new RWO(docUri, rwoLabel);
	}
}
