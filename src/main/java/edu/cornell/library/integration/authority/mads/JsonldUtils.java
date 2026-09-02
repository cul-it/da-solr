package edu.cornell.library.integration.authority.mads;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.apicatalog.jsonld.JsonLd;
import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.JsonLdErrorCode;
import com.apicatalog.jsonld.document.Document;
import com.apicatalog.jsonld.document.JsonDocument;

import edu.cornell.library.integration.authority.AuthoritySource;
import static edu.cornell.library.integration.authority.mads.Constants.CONTEXT_PREFIXES;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonString;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;
import jakarta.json.JsonWriter;

public class JsonldUtils {
	public static int countNumUpdate(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = JsonldUtils.getIdAsList(adminMds);
		return adminMdIds.size();
	}

	public static AuthoritySource extractVocab(String id) {
		if (id.startsWith("gf")) return AuthoritySource.LCGFT;
		if (id.startsWith("sj")) return AuthoritySource.LCJSH;
		if (id.startsWith("sh")) return AuthoritySource.LCSH;
		if (id.startsWith("n"))  return AuthoritySource.NAF;
		return AuthoritySource.UNK;
	}

	public static edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource translateAuthoritySource(AuthoritySource source) {
		return switch (source) {
			// case LC -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.LC;
			case NAF -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.LCNAF;
			case LCSH -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.LCSH;
			case LCGFT -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.LCGFT;
			// case LCJSH -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.LCJSH;
			// case FAST -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.FAST;
			// case OTHER -> edu.cornell.library.integration.metadata.support.AuthorityData.AuthoritySource.OTHER;
			default -> null;
		};
	}

	public static List<String> asList(JsonObject node, String key) {
		List<String> list = new ArrayList<>();
		JsonValue value = node.get(key);
		if (value == null)
			return list;
		if (null != value.getValueType()) switch (value.getValueType()) {
                case ARRAY -> {
                    for (JsonValue val : value.asJsonArray()) {
                        if (val.getValueType() == ValueType.OBJECT)
                            list.add(val.asJsonObject().getJsonString("@id").getString());
                        else
                            list.add(((JsonString) val).getString());
                    }
                    }
                case OBJECT -> list.add(value.asJsonObject().getJsonString("@id").getString());
                case STRING -> list.add(((JsonString) value).getString());
                default -> {
                    }
            }
		return list;
	}

	/*
	 * For madsrdf:authoritativeLabel
	 * 1. if it is null, return null (happens for deprecated item)
	 * 2. if it is a string, return it
	 * 3. if it is a list, return the first string representation
	 *    if string representation is not found on the list, return @value from last object representation
	 * 4. if it is an object (map), return the @value
	 * If above doesn't resolve to anything, return null
	 */
	public static String getAuthorativeLabel(JsonObject mainEntry) {
		JsonValue authLabel = mainEntry.get("madsrdf:authoritativeLabel");
		if (authLabel == null)
			return null;
		if (authLabel.getValueType() == ValueType.STRING)
			return JsonldUtils.getString(authLabel);
		String labelForLang = null;
		if (authLabel.getValueType() == ValueType.ARRAY) {
			for (JsonValue label : authLabel.asJsonArray()) {
				if (label.getValueType() == ValueType.STRING)
					return JsonldUtils.getString(label);
				else if (label.getValueType() == ValueType.OBJECT)
					labelForLang = label.asJsonObject().getJsonString("@value").getString();
			}
		} else if (authLabel.getValueType() == ValueType.OBJECT)
			return authLabel.asJsonObject().getString("@value");
		return labelForLang;
	}

	public static List<String> getAuthorativeLabels(JsonObject mainEntry) {
		JsonValue authLabel = mainEntry.get("madsrdf:authoritativeLabel");
		if (authLabel == null)
			return null;
		if (authLabel.getValueType() == ValueType.STRING)
			return Arrays.asList(JsonldUtils.getString(authLabel));
		List<String> labels = new ArrayList<>();
		if (authLabel.getValueType() == ValueType.ARRAY) {
			for (JsonValue label : authLabel.asJsonArray()) {
				if (label.getValueType() == ValueType.STRING)
					labels.add(JsonldUtils.getString(label));
				else if (label.getValueType() == ValueType.OBJECT)
					labels.add(label.asJsonObject().getJsonString("@value").getString());
			}
		} else if (authLabel.getValueType() == ValueType.OBJECT)
			labels.add(authLabel.asJsonObject().getString("@value"));
		return labels;
	}

	public static List<String> getIdAsList(JsonValue arg) {
		List<String> ids = new ArrayList<>();
		if (arg == null) return ids;

		if (arg.getValueType() == ValueType.ARRAY) {
			for (JsonValue val : arg.asJsonArray())
				ids.add(val.asJsonObject().getJsonString("@id").getString());
		} else if (arg.getValueType() == ValueType.OBJECT) {
			var list = arg.asJsonObject().getJsonArray("@list");
			if (list != null)
				for (var val : list)
					ids.add(val.asJsonObject().getJsonString("@id").getString());
			else ids.add(arg.asJsonObject().getJsonString("@id").getString());
		}
		return ids;
	}

	public static JsonObject getJsonObjectForId(JsonArray graph, String id) {
		for (JsonValue entry : graph) {
			JsonObject obj = entry.asJsonObject();
			String entryId = obj.getString("@id");
			if (id.equalsIgnoreCase(entryId))
				return obj;
		}
		return JsonValue.EMPTY_JSON_OBJECT;
	}

	public static JsonObject getLatestRecordInfo(JsonArray graph, JsonObject mainEntry) {
		JsonValue adminMds = mainEntry.get("madsrdf:adminMetadata");
		List<String> adminMdIds = JsonldUtils.getIdAsList(adminMds);
		JsonObject recordInfo = JsonValue.EMPTY_JSON_OBJECT;
		String latest = null;
		for (String adminMdId : adminMdIds) {
			JsonObject ri = JsonldUtils.getJsonObjectForId(graph, adminMdId);
			if (latest == null) {
				recordInfo = ri;
				latest = recordInfo.getJsonObject("ri:recordChangeDate").getJsonString("@value").getString();
			} else {
				String changeDate = ri.getJsonObject("ri:recordChangeDate").getJsonString("@value").getString();
				if (changeDate.compareToIgnoreCase(latest) > 0) {
					recordInfo = ri;
					latest = changeDate;
				}
			}
		}
		return recordInfo;
	}

	public static List<JsonObject> getRwos(JsonObject node, JsonArray graph) {
		List<JsonObject> rwos = new ArrayList<>();
		JsonValue rwosVal = node.get("madsrdf:identifiesRWO");
		List<String> rwoIds = getIdAsList(rwosVal);
		for (String rwoId : rwoIds) {
			JsonObject rwo = getJsonObjectForId(graph, rwoId);
			if (rwo.isEmpty()) continue;

			rwos.add(rwo);
		}
		return rwos;
	}

	// Get string value or null if key doesn't exist or not a string type
	public static String getString(JsonObject obj, String key) {
		JsonValue value = obj.get(key);
		if (value == null)
			return null;

		return getString(value);
	}

	/*
	 * Shorthand to get string value from a generic type JsonValue
	 * If it is an object, return the @value as string
	 * If it is an array, return the first element as string
	 */
	public static String getString(JsonValue value) {
		if (null != value.getValueType())
			switch (value.getValueType()) {
                case STRING -> {
                    return ((JsonString) value).getString();
                    }
                case OBJECT -> {
                    return value.asJsonObject().getJsonString("@value").getString();
                    }
                case ARRAY -> {
                    return getString(value.asJsonArray().get(0));
                    }
                default -> {
                    }
            }
		return null;
	}

	public static HeadingType headingType(JsonObject mainEntry, String docUri) {
		List<HeadingType> types = parseHeadingType(mainEntry);
		switch (types.size()) {
			case 0 -> {
				System.out.println("Identified no Mads heading type for " + docUri);
				return null;
			}
			case 1 -> {
				return types.get(0);
			}
			default -> {
				System.out.println("Identified multiple Mads heading types for " + docUri + " : " + types);
				return types.get(0);
			}
		}
	}

	public static AuthorityData parseAuthorityData(JsonObject doc) throws JsonLdError, URISyntaxException, IOException {
		return parseAuthorityData(doc, null);
	}

	public static AuthorityData parseAuthorityData(JsonObject doc, String docUri) throws JsonLdError, URISyntaxException, IOException {
		JsonArray graph = doc.getJsonArray("@graph");
		if (docUri == null) {
			docUri = "http://id.loc.gov";
			var val = doc.getJsonString("@id");
			// bulk download format has @id at the root level
			if (val != null)
				docUri += doc.getJsonString("@id").getString();
			else
				throw new JsonLdError(JsonLdErrorCode.UNSPECIFIED, "ID not found for bulk download entry");
		} else
			docUri = docUri.replace("https://", "http://");

		AuthorityData parsed = new AuthorityData();
		JsonObject mainEntry = getJsonObjectForId(graph, docUri);

		parsed.id = mainEntry.getJsonString("@id").getString();
		parsed.lccn = getString(mainEntry, "identifiers:lccn");

		String path = new URI(parsed.id).getPath();
		int lastSlashIndex = path.lastIndexOf('/');
		parsed.vocab = extractVocab(path.substring(lastSlashIndex + 1));

		JsonValue isMemberOf = mainEntry.get("madsrdf:isMemberOfMADSCollection");
		parsed.undifferentiated = parseUndifferentiated(isMemberOf);
		parsed.isSubdivision = parseIsSubdivision(isMemberOf);

		parsed.authorativeLabel = getAuthorativeLabel(mainEntry);
		// If subdivision, we will try to replace -- to >
		if (parsed.isSubdivision)
			parsed.authorativeLabel = parsed.authorativeLabel.replace("--", " > ");

		parsed.headingType = headingType(mainEntry, docUri);

		JsonObject riRecord = getLatestRecordInfo(graph, mainEntry);
		parsed.moddate = getString(riRecord, "ri:recordChangeDate");

		String recordStatus = getString(riRecord, "ri:recordStatus");
		parsed.recordStatus = RecordStatus.byName(recordStatus); // can this be null?
		parsed.numUpdates = countNumUpdate(graph, mainEntry);

		try (StringWriter sw = new StringWriter();
			 JsonWriter jsonWriter = Json.createWriter(sw)) {
			jsonWriter.write(doc);
			parsed.source = sw.toString();
		}

		return parsed;
	}

	public static AuthorityData parseAuthorityData(String jsonld) throws JsonLdError, URISyntaxException, IOException {
		JsonObject doc = parseJsonLd(jsonld);
		return parseAuthorityData(doc);
	}

	public static AuthorityData parseAuthorityData(String jsonld, String id) throws JsonLdError, URISyntaxException, IOException {
		JsonObject doc = parseFlatJsonLd(jsonld, id);
		return parseAuthorityData(doc, id);
	}

	public static ComponentList parseComponentList(JsonObject entry, JsonArray graph) {
		ComponentList componentList = new ComponentList();
		JsonValue list = entry.get("madsrdf:componentList");
		if (list == null)
			return componentList;

		List<String> ids = getIdAsList(list);
		for (String id : ids) {
			JsonObject compObj = getJsonObjectForId(graph, id);
			HeadingType headingType = parseHeadingType(compObj).get(0);
			String authorativeLabel = getAuthorativeLabel(compObj);
			componentList.addComponent(id, headingType, authorativeLabel);
		}
		return componentList;
	}

	public static List<String> parseElementList(JsonArray graph, JsonObject entry) {
		List<String> elements = new ArrayList<>();
		JsonValue list = entry.get("madsrdf:elementList");
		if (list == null)
			return elements;

		List<String> ids = getIdAsList(list);
		for (String id : ids) {
			var elementObj = getJsonObjectForId(graph, id);
			if (elementObj.isEmpty()) continue;
			String elementValue = getString(elementObj, "madsrdf:elementValue");
			if (elementValue == null) continue;
			elements.add(id);
		}
		return elements;
	}

	public static List<HeadingType> parseHeadingType(JsonObject mainEntry) {
		JsonValue ht = mainEntry.get("@type");
		List<HeadingType> types = new ArrayList<>();
		if (ht.getValueType() == ValueType.ARRAY) {
			for (JsonValue type : ht.asJsonArray()) {
				HeadingType t = HeadingType.byType(JsonldUtils.getString(type));
				if (t != null)
					types.add(t);
			}
		} else if (ht.getValueType() == ValueType.STRING) {
			HeadingType t = HeadingType.byType(JsonldUtils.getString(ht));
			if (t != null)
				types.add(t);
		}
		return types;
	}

	public static boolean parseIsMemberOfMADSCollection(JsonValue isMemberOf, String idUrl) {
		if (isMemberOf == null)
			return false;
		if (null != isMemberOf.getValueType())
			switch (isMemberOf.getValueType()) {
                case STRING -> {
                    return idUrl.equalsIgnoreCase(getString(isMemberOf));
                    }
                case OBJECT -> {
                    return idUrl.equalsIgnoreCase(getString(isMemberOf.asJsonObject(), "@id"));
                    }
                case ARRAY -> {
                    for (JsonValue memberOf : isMemberOf.asJsonArray()) {
                        if (idUrl.equalsIgnoreCase(memberOf.asJsonObject().getJsonString("@id").getString()))
                            return true;
                    }
                    }
                default -> {
                    }
            }
		return false;
	}

	public static boolean parseIsSubdivision(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.SUBDIV_URL);
	}

	public static JsonObject parseJsonLd(InputStream is) throws JsonLdError {
		Document document = JsonDocument.of(is);
		return parseJsonLd(document);
	}

	public static JsonObject parseJsonLd(String jsonld) throws JsonLdError {
		StringReader reader = new StringReader(jsonld);
		Document document = JsonDocument.of(reader);
		return parseJsonLd(document);
	}

	public static JsonObject parseJsonLd(Document document) throws JsonLdError {
		Optional<JsonStructure> jsonContentOptional = document.getJsonContent();
		if (jsonContentOptional.isPresent()) {
			JsonStructure jsonContent = jsonContentOptional.get();
			if (jsonContent.getValueType() == ValueType.OBJECT)
				return jsonContent.asJsonObject();
		}

		throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Failed to parse jsonld");
	}

	public static JsonObject parseFlatJsonLd(String content, String id) throws JsonLdError {
		try (InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
			return parseFlatJsonLd(is, id);
		} catch (IOException e) {
			throw new JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Failed to parse jsonld for " + id, e);
		}
	}

	public static JsonObject parseFlatJsonLd(InputStream is, String id) throws JsonLdError {
		Document doc = JsonDocument.of(is);
		return JsonLd.compact(doc, getContext(id)).get();
	}

	public static String getContext(String id) {
		for (String prefix : CONTEXT_PREFIXES) {
			if (id.startsWith(prefix))
				return prefix + "context.json";
		}
		return "http://id.loc.gov/authorities/names/context.json";
	}

	public static boolean parseUndifferentiated(JsonValue isMemberOf) {
		return parseIsMemberOfMADSCollection(isMemberOf, Constants.UNDIFF_URL);
	}

	public static String uri(String id) {
		if (id.startsWith("gf")) {
			return "http://id.loc.gov/authorities/genreForms/" + id;
		} else if (id.startsWith("n")) {
			return "http://id.loc.gov/authorities/names/" + id;
		} else if (id.startsWith("sh")) {
			return "http://id.loc.gov/authorities/subjects/" + id;
		}

		return null;
	}

	public static String id(String uri) {
		if (uri.startsWith("http://id.loc.gov/authorities/genreForms/")) {
			return uri.substring("http://id.loc.gov/authorities/genreForms/".length());
		} else if (uri.startsWith("http://id.loc.gov/authorities/names/")) {
			return uri.substring("http://id.loc.gov/authorities/names/".length());
		} else if (uri.startsWith("http://id.loc.gov/authorities/subjects/")) {
			return uri.substring("http://id.loc.gov/authorities/subjects/".length());
		}

		return null;
	}
}
