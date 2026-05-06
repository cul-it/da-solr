package edu.cornell.library.integration.authority.activitystreams;

import java.util.Map;

public class ActivityStreamsParams {
	static final Map<String, ActivityStreamsParamsEntry> PARAMS_MAP = Map.ofEntries(
			Map.entry("names", new ActivityStreamsParamsEntry(
					"https://id.loc.gov/authorities/names/context.json",
					"https://id.loc.gov/authorities/names/activitystreams/feed/1.json"
					)),
			Map.entry("subjects", new ActivityStreamsParamsEntry(
					"https://id.loc.gov/authorities/subjects/context.json",
					"https://id.loc.gov/authorities/subjects/activitystreams/feed/1.json"
					))
		);

	public static ActivityStreamsParamsEntry getParam(String type) {
		return PARAMS_MAP.get(type);
	}
}
