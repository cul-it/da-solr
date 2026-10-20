package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.apicatalog.jsonld.JsonLdError;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

public class Notes {
	static final ObjectMapper mapper = new ObjectMapper();

	public static List<String> notes(Connection authority, JsonObject mainEntry, JsonArray graph) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		List<String> notes = new ArrayList<>();

		List<String> ids = JsonldUtils.getIdAsList(mainEntry.get("madsrdf:see"));
		if (ids.isEmpty()) return notes;

		List<Map<String, String>> noteDataList = new ArrayList<>();
		for (String id : ids) {
			Map<String, String> noteData = new HashMap<>();
			if (id.startsWith("http://id.loc.gov/")) {
				var data = DbUtils.getOrFetchAuthorityRecord(authority, id);
				noteData.put("header", data.authorativeLabel);
			} else {
				var noteNode = JsonldUtils.getJsonObjectForId(graph, id);
				noteData.put("header", JsonldUtils.getAuthorativeLabel(noteNode));
			}
			noteDataList.add(noteData);
		}
		notes.add("Search also under: " + mapper.writeValueAsString(noteDataList));

		return notes;
	}
}
