package edu.cornell.library.integration.authority.mads;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.apicatalog.jsonld.JsonLdError;

import edu.cornell.library.integration.authority.mads.ComponentList.Component;
import edu.cornell.library.integration.utilities.FilingNormalization;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;

public record Heading(AtomicReference<Integer> id, Heading parent, String heading, String sort, HeadingType headingType) {
	private static final String PADDING = "  ";
	public void print(String padding) {
		if (parent != null) {
			System.out.println(padding + "parent start");
			parent.print(padding + PADDING);
			System.out.println(padding + "parent end");
		}
		System.out.println(padding + "heading: " + heading);
		System.out.println(padding + "sort: " + sort);
		System.out.println(padding + "heading type: " + headingType);
		System.out.println(padding + "----");
	}

	public Heading(Heading parent, String heading, String sort, HeadingType headingType) {
		this(new AtomicReference<>(0), parent, heading, sort, headingType);
	}

	public Heading(String heading, String sort, HeadingType headingType) {
		this(new AtomicReference<>(0), null, heading, sort, headingType);
	}

	public int setHeadingId(int id) {
		this.id.set(id);
		return id;
	}

	public int headingId() {
		return this.id.get();
	}

	public int parentId() {
		if (parent == null) return 0;
		return parent.headingId();
	}

	protected static String prettyHeading(String label) {
		if (label == null) return null;
		/*
			If the label contains "--" (double dash) with any number of whitespace before or after, replace it including the whitespaces with " > " (space, greater-than symbol, space).
			Otherwise, return the label unchanged.
		 */
		return label.replaceAll("\\s*--\\s*", " > ");
	}

	protected static String acronym(String heading, String mainHeading) {
		if (mainHeading == null) return heading;

		int capitalCount = 0;
		for (char c : heading.toCharArray()) {
			if ( Character.isUpperCase(c) ) capitalCount++;
			else if ( c != '.' ) return heading;

			if (capitalCount > 5) break;
		}

		return heading+" ("+mainHeading+")";
	}

	public static Heading processHeading(Connection authority, JsonObject entry, JsonArray graph, String heading, String mainHeading, HeadingType ht) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		// Emulates processHeadingField's parent heading logic for AUTHORTITLE fields:
		// when the heading type is NAME_TITLE, the first component (the name/author part)
		// becomes a parent HeadingMap, mirroring how MARC builds a parent Heading for WORK entries.
		Heading parent = null;
		if (ht == HeadingType.NAME_TITLE) {
			ComponentList componentList = JsonldUtils.parseComponentList(entry, graph);
			if (componentList.components.isEmpty()) {
				System.out.println("No components found for NAME_TITLE heading: " + heading);
				return new Heading(heading, FilingNormalization.getFilingForm(heading), ht);
			}
			var first = firstComponent(authority, componentList);
			if (first != null) {
				heading = constructHeadingFromComponentList(authority, heading, first, componentList);
				if (first.authorativeLabel != null && first.headingType != null)
					parent = new Heading(first.authorativeLabel, FilingNormalization.getFilingForm(first.authorativeLabel), first.headingType);
			}
		}

		heading = prettyHeading(heading);
		heading = acronym(heading, mainHeading);
		return new Heading(parent, heading, FilingNormalization.getFilingForm(heading), ht);
	}

	protected static Component firstComponent(Connection authority, ComponentList componentList) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		if (componentList.components.isEmpty()) return null;

		var first = componentList.first();
		if ((first.authorativeLabel == null || first.authorativeLabel.isBlank())
				&& first.id.startsWith("http://id.loc.gov")) {
			var comp = DbUtils.getOrFetchAuthorityRecord(authority, first.id);
			var authLabel = comp.authorativeLabel;
			first.authorativeLabel = authLabel;
			first.headingType = JsonldUtils.parseHeadingType(comp.mainEntry).get(0);
		}
		return first;
	}

	protected static String constructHeadingFromComponentList(Connection authority, String heading, Component first, ComponentList componentList) throws SQLException, JsonLdError, IOException, URISyntaxException, InterruptedException {
		if (first == null) return heading;

		StringBuilder sb = new StringBuilder(first.authorativeLabel);
		List<String> restLabels = new ArrayList<>();
		for (var component : componentList.restComponents()) {
			if (component.authorativeLabel == null || component.authorativeLabel.isBlank()) {
				if (component.id.startsWith("http://id.loc.gov")) {
					var comp = DbUtils.getOrFetchAuthorityRecord(authority, component.id);
					var authLabel = comp.authorativeLabel;
					if (authLabel != null && !authLabel.isBlank())
						restLabels.add(authLabel);
					else
						System.out.println("No label found for component: " + component.id);
				}
				continue;
			}
			restLabels.add(component.authorativeLabel);
		}
		String rest = String.join(" -- ", restLabels);
		if (!rest.isBlank()) sb.append(" | ").append(rest);

		return sb.toString();
	}
}
