package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.apicatalog.jsonld.JsonLdError;

import static edu.cornell.library.integration.authority.mads.Constants.HAS_BROADER_AUTHORITY;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_EARLIER_ESTABLISHED_FORM;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_LATER_ESTABLISHED_FORM;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_NARROWER_AUTHORITY;
import static edu.cornell.library.integration.authority.mads.Constants.HAS_RELATED_AUTHORITY;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

public class References {
	protected static final String PADDING = "  ";
	public static final List<String> RELATIONSHIP_KEYS = List.of(
			HAS_BROADER_AUTHORITY,
			HAS_EARLIER_ESTABLISHED_FORM,
			HAS_LATER_ESTABLISHED_FORM,
			HAS_NARROWER_AUTHORITY,
			HAS_RELATED_AUTHORITY
	);

	/**
	 * Returns a list of "see" relationships for the given main heading.
	 * This method identifies all variant forms of the main heading that should be displayed as "see" references, excluding those that are already established as earlier forms.
	 * 
	 * @param authority
	 * @param mainEntry
	 * @param graph
	 * @param lcId
	 * @param mainHead
	 * @return a list of "see" relationships for the main heading
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SQLException
	 * @throws JsonLdError
	 * @throws URISyntaxException
	 */
	public static List<Relationship> sees(Connection authority, JsonObject mainEntry, JsonArray graph, String lcId, Heading mainHead) throws IOException, InterruptedException, SQLException, JsonLdError, URISyntaxException {
		Set<String> seen = new HashSet<>();
		seen.add(fingerprint(mainHead));
		List<Relationship> sees = new ArrayList<>();

		List<String> variantIds = getVariants(mainEntry);
		List<String> earlierLabels = earlierLabels(graph, mainEntry);
		for (String variantId : variantIds) {
			JsonObject variant = JsonldUtils.getJsonObjectForId(graph, variantId);
			if (variant == null || variant.isEmpty()) continue;

			String label = variantLabel(graph, variant);
			if (label == null || label.isBlank()) continue;
			boolean display = !earlierLabels.contains(label);

			HeadingType vt = JsonldUtils.headingType(variant, lcId);
			var h = Heading.processHeading(authority, variant, graph, label, mainHead.heading(), vt);
			if (seen.contains(fingerprint(h))) continue;
			seen.add(fingerprint(h));

			Relationship rel = new Relationship(null, null, h, display);
			sees.add(rel);
		}
		return sees;
	}

	private static String fingerprint(Heading heading) {
		String parent = (heading.parent() == null) ? "" : heading.parent().heading() + "|" + heading.parent().headingType() + "|" + heading.parent().sort();
		return heading.heading() + "|" + heading.headingType() + "|" + heading.sort() + parent;
	}

	protected static List<String> getVariants(JsonObject mainEntry) {
		return JsonldUtils.asList(mainEntry, "madsrdf:hasVariant");
	}

	protected static String variantLabel(JsonArray graph, JsonObject node) {
		String label = JsonldUtils.getString(node, "madsrdf:variantLabel");
		if (label != null) return label;
		List<String> elementValues = JsonldUtils.parseElementList(graph, node);
		return String.join(" ", elementValues);
	}

	protected static List<String> earlierLabels(JsonArray graph, JsonObject node) {
		List<String> labels = new ArrayList<>();
		var earler = node.get(HAS_EARLIER_ESTABLISHED_FORM);
		if (earler == null) return labels;
		List<String> earlierIds = JsonldUtils.getIdAsList(earler);
		for (String earlierId : earlierIds) {
			JsonObject earlierNode = JsonldUtils.getJsonObjectForId(graph, earlierId);
			String label = variantLabel(graph, earlierNode);
			if (label != null && !label.isBlank())
				labels.add(label);
		}
		return labels;
	}

	/**
	 * Returns a list of "see also" relationships for the given main heading.
	 * It iterates over RELATIONSHIP_KEYS to find related authority records and constructs the corresponding "see also" relationships.
	 * 
	 * @param authority
	 * @param mainEntry
	 * @param graph
	 * @param lcId
	 * @param mainHead
	 * @return a list of "see also" relationships for the main heading
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws JsonLdError
	 * @throws URISyntaxException
	 * @throws SQLException
	 */
	public static List<Relationship> seeAlsos(Connection authority, JsonObject mainEntry, JsonArray graph, String lcId, Heading mainHead) throws IOException, InterruptedException, JsonLdError, URISyntaxException, SQLException {
		Set<String> seen = new HashSet<>();
		seen.add(fingerprint(mainHead));
		List<Relationship> seeAlsos = new ArrayList<>();

		for (String relationshipKey : RELATIONSHIP_KEYS) {
			var related = JsonldUtils.asList(mainEntry, relationshipKey);
			if (related.isEmpty()) continue;

			for (String relatedAuthId : related) {
				if (relatedAuthId == null || relatedAuthId.isBlank()) continue;
				String label;
				JsonArray relatedGraph;
				if (relatedAuthId.startsWith("http://id.loc.gov/")) {
					var relatedAuth = DbUtils.getOrFetchAuthorityRecord(authority, relatedAuthId);
					label = relatedAuth.authorativeLabel;
					relatedGraph = relatedAuth.graph;
				} else {
					label = JsonldUtils.getAuthorativeLabel(JsonldUtils.getJsonObjectForId(graph, relatedAuthId));
					relatedGraph = graph;
				}
				if (label == null || label.isBlank()) continue;

				JsonObject relatedNode = JsonldUtils.getJsonObjectForId(relatedGraph, relatedAuthId);
				HeadingType ht = JsonldUtils.headingType(relatedNode, relatedAuthId);
				Heading h = Heading.processHeading(authority, relatedNode, relatedGraph, label, null, ht);
				if (seen.contains(fingerprint(h))) continue;
				seen.add(fingerprint(h));

				Relationship rel = determineRelationship(relationshipKey, h);
				seeAlsos.add(rel);
			}
		}
		return seeAlsos;
	}

	public record Relationship(String relationship, String reciprocalRelationship, Heading heading, boolean display) {
		public void print(String padding) {
			System.out.println(padding + "relationship: " + relationship);
			System.out.println(padding + "reciprocal: " + reciprocalRelationship);
			if (heading != null)
				heading.print(padding + PADDING);
			System.out.println(padding + "display: " + display);
			System.out.println(padding + "----");
		}
	}

	protected static Relationship determineRelationship(String relationshipKey, Heading heading) {
		switch (relationshipKey) {
			case HAS_BROADER_AUTHORITY -> {
				return new Relationship(forRelationshipKey(relationshipKey), forRelationshipKey(HAS_NARROWER_AUTHORITY), heading, true);
			}
			case HAS_NARROWER_AUTHORITY -> {
				return new Relationship(forRelationshipKey(relationshipKey), forRelationshipKey(HAS_BROADER_AUTHORITY), heading, true);
			}
			case HAS_EARLIER_ESTABLISHED_FORM -> {
				return new Relationship(forRelationshipKey(relationshipKey), forRelationshipKey(HAS_LATER_ESTABLISHED_FORM), heading, true);
			}
			case HAS_LATER_ESTABLISHED_FORM -> {
				return new Relationship(forRelationshipKey(relationshipKey), forRelationshipKey(HAS_EARLIER_ESTABLISHED_FORM), heading, true);
			}
			default -> {
				return new Relationship(null, null, heading, true);
			}


		}
	}

	protected static String forRelationshipKey(String relationshipKey) {
		return switch (relationshipKey) {
			case HAS_BROADER_AUTHORITY -> "Narrower Term";
			case HAS_NARROWER_AUTHORITY -> "Broader Term";
			case HAS_EARLIER_ESTABLISHED_FORM -> "Later Heading";
			case HAS_LATER_ESTABLISHED_FORM -> "Earlier Heading";
			case HAS_RELATED_AUTHORITY -> "Related Term";
			default -> null;
		};
	}
}
