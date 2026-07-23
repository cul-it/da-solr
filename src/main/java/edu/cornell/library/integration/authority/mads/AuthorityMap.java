package edu.cornell.library.integration.authority.mads;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.apicatalog.jsonld.JsonLdError;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.authority.activitystreams.AuthorityParsedData;
import edu.cornell.library.integration.authority.activitystreams.MadsHeadingType;
import edu.cornell.library.integration.authority.activitystreams.Utils;
import edu.cornell.library.integration.metadata.support.AuthorityData.ReferenceType;
import edu.cornell.library.integration.utilities.FilingNormalization;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;
import jakarta.json.JsonValue.ValueType;

public record AuthorityMap(int id, String lccn, String heading, String localId, boolean undifferentiated, List<HeadingMap> headings, List<ReferenceMap> references, String rda) {
	static Pattern subdivisionPattern = Pattern.compile("(\\S+)--(\\S+)$");

	public void print() {
		System.out.println("lccn: " + lccn);
		System.out.println("heading: " + heading);
		System.out.println("localId: " + localId);
		System.out.println("undifferentiated: " + undifferentiated);
		for (HeadingMap heading : headings)
			heading.print("  ");
		for (ReferenceMap ref : references)
			ref.print("  ");
	}

	public record HeadingMap(int id, int parentId, String heading, String sort, MadsHeadingType headingType, int worksBy, int worksAbout, int works, boolean mainEntry, ReferenceType refType) {
		public void print(String padding) {
			System.out.println(padding + "parent id: " + parentId);
			System.out.println(padding + "heading: " + heading);
			System.out.println(padding + "sort: " + sort);
			System.out.println(padding + "heading type: " + headingType);
			System.out.println(padding + "works by: " + worksBy);
			System.out.println(padding + "worksAbout: " + worksAbout);
			System.out.println(padding + "works: " + works);
			System.out.println(padding + "main entry: " + mainEntry);
			System.out.println(padding + "reference type: " + refType);
			System.out.println(padding + "----");
		}
	}

	public record ReferenceMap(int id, HeadingMap fromHeading, HeadingMap toHeading, ReferenceType refType, String refDesc) {
		public void print(String padding) {
			System.out.println(padding + "from heading: " + fromHeading.heading);
			System.out.println(padding + "to heading: " + toHeading.heading);
			System.out.println(padding + "ref type: " + refType.toString());
			System.out.println(padding + "ref desc: " + refDesc);
			System.out.println(padding + "----");
		}
	}

	public static List<String> getIdentifiesRWO(JsonObject node) {
		return Utils.getListForArray(node, "madsrdf:identifiesRWO");
	}

	public static List<String> getRelatedAuthority(JsonObject mainEntry) {
		return Utils.getListForArray(mainEntry, "madsrdf:hasRelatedAuthority");
	}

	public static List<String> getVariants(JsonObject mainEntry) {
		return Utils.getListForArray(mainEntry, "madsrdf:hasVariant");
	}

	public static AuthorityMap fromMadsJsonld(Connection authority, String lcId, String source, boolean undifferentiated) throws JsonLdError, JsonProcessingException, SQLException {
		System.out.println(source);
		JsonObject doc = Utils.parseJsonLd(source);
		JsonArray graph = doc.getJsonArray("@graph");
		JsonObject mainEntry = Utils.getJsonObjectForId(graph, lcId);
		String lccn = Utils.getString(mainEntry, "identifiers:lccn");
		String localId = Utils.getString(mainEntry, "identifiers:local");

		List<HeadingMap> headings = populateHeadings(authority, mainEntry, graph, lcId);

		List<ReferenceMap> references = populateReferences(authority, headings);

		String heading = null;
		for (HeadingMap map : headings)
			if (map.mainEntry) {
				heading = map.heading;
				break;
			}

		Rda rda = new Rda();
		Map<String,Collection<String>> data = rda.populateRda(authority, mainEntry, graph);

		return new AuthorityMap(0, lccn, heading, localId, undifferentiated, headings, references, Rda.json(data));
	}

	protected static List<HeadingMap> populateHeadings(Connection authority, JsonObject mainEntry, JsonArray graph, String lcId) throws JsonProcessingException, SQLException {
		List<HeadingMap> headings = new ArrayList<>();

		String authLabel = headingLabel(Utils.getAuthorativeLabel(mainEntry));
		MadsHeadingType t = Utils.headingType(mainEntry, lcId);
		HeadingMap authHeading = new HeadingMap(0, 0, authLabel, FilingNormalization.getFilingForm(authLabel), t, 0, 0, 0, true, null);
		headings.add(authHeading);

		List<String> variantIds = getVariants(mainEntry);
		for (String variantId : variantIds) {
			JsonObject variant = Utils.getJsonObjectForId(graph, variantId);
			if (variant == null) continue;
			
			String label = headingLabel(Utils.getString(variant, "madsrdf:variantLabel"));
			MadsHeadingType vt = Utils.headingType(variant, lcId);
			HeadingMap varMap = new HeadingMap(0, 0, label, FilingNormalization.getFilingForm(label), vt, 0, 0, 0, false, ReferenceType.FROM4XX);
			headings.add(varMap);
		}
		List<String> realtedAuths = getRelatedAuthority(mainEntry);
		for (String realtedAuthId : realtedAuths) {
			AuthorityParsedData relatedAuth = Utils.headingFromLcId(authority, realtedAuthId);
			if (relatedAuth != null) {
				String label = headingLabel(relatedAuth.authorativeLabel);
				HeadingMap relMap = new HeadingMap(0, 0, label, FilingNormalization.getFilingForm(label), relatedAuth.headingType, 0, 0, 0, false, ReferenceType.FROM5XX);
				headings.add(relMap);
			}
		}
		return headings;
	}

	protected static List<ReferenceMap> populateReferences(Connection authority, List<HeadingMap> headings) {
		List<ReferenceMap> references = new ArrayList<ReferenceMap>();
		if (headings.size() == 1)
			return references;
		HeadingMap to = headings.get(0);
		for (int i = 1; i < headings.size(); i++) {
			HeadingMap from = headings.get(i);
			// how to get ref_desc string (last argument)?
			ReferenceMap ref = new ReferenceMap(0, from, to, from.refType, "");
			references.add(ref);
		}
		return references;
	}

	protected static String headingLabel(String label) {
		Matcher m = subdivisionPattern.matcher(label);
		if ( m.matches() ) {
			return label.replace("--", " > ");
		}
		return label;
	}
	static final ObjectMapper mapper = new ObjectMapper();
	static class Rda {
		Map<String, String> fieldMap = new LinkedHashMap<>();
		Map<String, Boolean> recursiveChecker = null;

		public Rda() {
			fieldMap.put("Country", "madsrdf:associatedLocale");
			fieldMap.put("Field", "madsrdf:fieldOfActivity");
			fieldMap.put("Group/Organization", "madsrdf:hasAffiliation");
			fieldMap.put("Occupation", "madsrdf:occupation");
		}

		public Map<String,Collection<String>> populateRda(Connection authority, JsonObject mainEntry, JsonArray graph) throws JsonProcessingException, SQLException {
			Map<String,Collection<String>> data = new HashMap<>();
			recursiveChecker = new HashMap<>();
			List<JsonObject> rwos = Utils.getRwos(mainEntry, graph);
			for (JsonObject rwo : rwos) {
				System.out.println(rwo.toString());
				JsonValue birthPlace = rwo.get("madsrdf:birthPlace");
				if (birthPlace != null) {
					List<String> ids = Utils.getIdAsList(birthPlace);
					for (String id : ids) {
						String bp = followBlankNode(authority, graph, id);
						if (bp != null)
							add(data, "Birth Place", bp);
					}
				}
				for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
					JsonValue rwoVal = rwo.get(entry.getValue());
					if (rwoVal == null) continue;

					switch (rwoVal.getValueType()) {
					case ARRAY:
					case OBJECT:
						List<String> ids = Utils.getIdAsList(rwoVal);
						for (String id : ids) {
							String value = followBlankNode(authority, graph, id);
							if (value != null)
								add(data, entry.getKey(), value);
						}
						break;
					case STRING:
						add(data, entry.getKey(), Utils.getString(rwoVal)); break;
					default:
						break;
					}
				}
			}

			return data;
		}

		public String followBlankNode(Connection authority, JsonArray graph, String blankNodeId) throws SQLException {
			if (recursiveChecker.containsKey(blankNodeId)) {
				// cyclic pointer found, ignore this key
				return null;
			}

			recursiveChecker.put(blankNodeId, true);

			JsonObject obj = Utils.getJsonObjectForId(graph, blankNodeId);
			if (obj.isEmpty())
				if (blankNodeId.startsWith("http://")) {
					AuthorityParsedData rec = Utils.headingFromLcId(authority, blankNodeId);
					if (rec != null)
						return rec.authorativeLabel;
					else
						return blankNodeId;
				}
				else {
					return null;
				}

			// https://id.loc.gov/authorities/names/n00000203
			// has Occupation "Author" which is not from a URI
			String label = Utils.getString(obj, "rdfs:label");
			if (label != null) return label;

			for (String key : obj.keySet()) {
				if (key == "@id") continue;
				JsonValue val = obj.get(key);
				if (val.getValueType() == ValueType.OBJECT) {
					return followBlankNode(authority, graph, Utils.getString(val.asJsonObject(), "@id"));
				}
			}

			return null;
		}

		protected void add(Map<String,Collection<String>> data, String field, String value) {
			if ( ! data.containsKey(field))
				data.put(field, new HashSet<String>());
			data.get(field).add(value);
		}

		public static String json(Map<String,Collection<String>> data) throws JsonProcessingException {
			if (data.isEmpty()) return null;
			return mapper.writeValueAsString(data);
		}
	}
}
