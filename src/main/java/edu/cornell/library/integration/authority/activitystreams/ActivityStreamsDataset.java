package edu.cornell.library.integration.authority.activitystreams;

import java.util.Map;

public class ActivityStreamsDataset {
	static final Map<String, ActivityStreamsDatasetEntry> DATASET_MAP = Map.ofEntries(
			Map.entry("names", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/names/context.json",
					"https://id.loc.gov/authorities/names/activitystreams/feed/1.json"
					)),
			Map.entry("subjects", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/subjects/context.json",
					"https://id.loc.gov/authorities/subjects/activitystreams/feed/1.json"
					))
		);

	public static ActivityStreamsDatasetEntry getParam(String type) {
		return DATASET_MAP.get(type);
	}
}
