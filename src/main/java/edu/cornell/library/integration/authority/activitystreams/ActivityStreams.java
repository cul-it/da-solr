package edu.cornell.library.integration.authority.activitystreams;

import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.authority.mads.JsonldUtils;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonValue;

public class ActivityStreams {
	public String id = null;
	public String next = null;
	public List<OrderedItem> orderedItems = new ArrayList<>();

	public void addOrderedItem(String id, String link) {
		orderedItems.add(new OrderedItem(id, link));
	}

	public class OrderedItem {
		public String id = null;
		public String link = null;
		public OrderedItem(String id, String link) {
			this.id = id;
			if (link == null) this.link = id + ".json";
			else this.link = link;
		}
	}

	public static String resolveLinkUrl(JsonArray url) {
		/*
		 * [{
		 *   "type": "Link",
		 *   "href": "http://id.loc.gov/authorities/subjects/sh2025001856.json",
		 *   "mediaType": "application/json"
		 * },
		 * ...
		 * ]
		 */
		for (JsonValue link : url) {
			JsonObject obj = link.asJsonObject();
			if ("Link".equalsIgnoreCase(JsonldUtils.getString(obj, "type")) && "application/json".equalsIgnoreCase(JsonldUtils.getString(obj, "mediaType"))) {
				return JsonldUtils.getString(obj, "href");
			}
		}
		return null;
	}
}
