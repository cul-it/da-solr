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

import edu.cornell.library.integration.authority.activitystreams.IFetcher;
import static edu.cornell.library.integration.authority.mads.Constants.ASSOCIATED_LOCALE;
import static edu.cornell.library.integration.authority.mads.Constants.BIRTH_PLACE;
import static edu.cornell.library.integration.authority.mads.Constants.DEATH_PLACE;
import static edu.cornell.library.integration.authority.mads.Constants.FIELD_OF_ACTIVITY;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_AFFILIATION;
import static edu.cornell.library.integration.authority.mads.Constants.OCCUPATION;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;

public record RWO(String id, String label, List<String> associatedLocale, List<String> birthPlace, List<String> deathPlace, List<String> fieldOfActivities, List<String> hasAffiliations, List<String> occupation) {
	public static IFetcher fetcher = new IFetcher.HttpFetcher();

	public void print(String padding) {
		System.out.println(padding + "RWO: " + id + " - " + label);
		System.out.println(padding + padding + "associated locale: " + associatedLocale);
		System.out.println(padding + padding + "birth place: " + birthPlace);
		System.out.println(padding + padding + "death place: " + deathPlace);
		System.out.println(padding + padding + "field of activities: " + fieldOfActivities);
		System.out.println(padding + padding + "has affiliations: " + hasAffiliations);
		System.out.println(padding + padding + "occupation: " + occupation);
	}

	/**
	 * Use this to parse RWO document from the whole JSON-LD document.
	 * The document first needs to be framed, such as by JsonldUtils.parseFlatJsonLd.
	 * 
	 * @param authority
	 * @param doc
	 * @param docUri
	 * @return
	 * @throws SQLException
	 * @throws JsonLdError
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	public static RWO fromJsonObject(Connection authority, JsonObject doc, String docUri) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		JsonArray graph = doc.getJsonArray("@graph");
		JsonObject mainEntry = JsonldUtils.getJsonObjectForId(graph, docUri);
		return parse(authority, mainEntry, graph, docUri);
	}

	/**
	 * Parse RWO from inside another JSON-LD document.
	 * 
	 * @param authority
	 * @param rwoEntry
	 * @param graph
	 * @param docUri
	 * @return
	 * @throws SQLException
	 * @throws JsonLdError
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException
	 */
	public static RWO parse(Connection authority, JsonObject rwoEntry, JsonArray graph,String docUri) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		var rwoLabel = JsonldUtils.getAuthorativeLabel(rwoEntry);
		if (rwoLabel == null || rwoLabel.isEmpty())
			rwoLabel = JsonldUtils.getString(rwoEntry, "rdfs:label");
		Map<String, Boolean> recursiveChecker = new HashMap<>();
		List<String> associatedLocale = parseKey(authority, graph, rwoEntry, ASSOCIATED_LOCALE, recursiveChecker);
		List<String> birthPlace = parseKey(authority, graph, rwoEntry, BIRTH_PLACE, recursiveChecker);
		List<String> deathPlace = parseKey(authority, graph, rwoEntry, DEATH_PLACE, recursiveChecker);
		List<String> fieldOfActivities = parseKey(authority, graph, rwoEntry, FIELD_OF_ACTIVITY, recursiveChecker);
		List<String> hasAffiliations = parseKey(authority, graph, rwoEntry, HAS_AFFILIATION, recursiveChecker);
		List<String> occupation = parseKey(authority, graph, rwoEntry, OCCUPATION, recursiveChecker);
		return new RWO(docUri, rwoLabel, associatedLocale, birthPlace, deathPlace, fieldOfActivities, hasAffiliations, occupation);
	}

	protected static List<String> parseKey(Connection authority, JsonArray graph, JsonObject rwoEntry, String key, Map<String, Boolean> recursiveChecker) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		List<String> values = new ArrayList<>();
		var val = rwoEntry.get(key);
		if (val == null) return values;

		switch(val.getValueType()) {
			case ARRAY, OBJECT -> {
				List<String> ids = JsonldUtils.getIdAsList(val);
				for (String id : ids) {
					String value = followNode(authority, graph, id, recursiveChecker);
					if (value != null)
						values.add(value);
				}
			}
			case STRING -> values.add(JsonldUtils.getString(val));
			default -> {}
		}

		return values;
	}

	protected static String followNode(Connection authority, JsonArray graph, String blankNodeId, Map<String, Boolean> recursiveChecker) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		if (recursiveChecker.containsKey(blankNodeId))
			// cyclic pointer found, ignore this key
			return null;

		recursiveChecker.put(blankNodeId, true);

		JsonObject obj = JsonldUtils.getJsonObjectForId(graph, blankNodeId);
		if (obj.isEmpty())
			if (blankNodeId.startsWith("http://id.loc.gov/rwo/"))
				return rwoLabel(authority, blankNodeId);
			else if (blankNodeId.startsWith("http://id.loc.gov/")) {
				var authData = DbUtils.getOrFetchAuthorityRecord(authority, blankNodeId);
				return authData.authorativeLabel;
			}
			else return null;

		// http://id.loc.gov/rwo/agents/n80001203
		// has madsrdf:authoritativeLabel
		String label = JsonldUtils.getAuthorativeLabel(obj);
		if (label != null) return label;
		// http://id.loc.gov/authorities/names/n00000203
		// has Occupation "Author" which is not from a URI as rdfs:label
		label = JsonldUtils.getString(obj, "rdfs:label");
		if (label != null) return label;

		for (String key : obj.keySet()) {
			if ("@id".equals(key)) continue;
			JsonValue val = obj.get(key);
			if (val.getValueType() == ValueType.OBJECT)
				return followNode(authority, graph, JsonldUtils.getString(val.asJsonObject(), "@id"), recursiveChecker);
		}

		return null;
	}

	public static String rwoLabel(Connection authority, String id) throws IOException, InterruptedException, JsonLdError, SQLException, URISyntaxException {
		RWO rwo = DbUtils.getOrFetchRwo(authority, id);
		if (rwo != null) return rwo.label();
		return null;
	}
}
