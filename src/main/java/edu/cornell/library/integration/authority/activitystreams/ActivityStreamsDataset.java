package edu.cornell.library.integration.authority.activitystreams;

import java.util.Map;

// LCSH, LCNAF, LCGFT, LCMPT, and RBMS
// RBMS doesn't seem to have any activity streams feed as of 2026/06/01

public class ActivityStreamsDataset {
	static final Map<String, ActivityStreamsDatasetEntry> DATASET_MAP = Map.ofEntries(
			Map.entry("LCNAF", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/names/context.json",
					"https://id.loc.gov/authorities/names/activitystreams/feed/1.json"
					)),
			Map.entry("LCSH", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/subjects/context.json",
					"https://id.loc.gov/authorities/subjects/activitystreams/feed/1.json"
					)),
			Map.entry("LCGFT", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/genreForms/context.json",
					"https://id.loc.gov/authorities/genreForms/activitystreams/feed/1.json"
					)),
			Map.entry("LCMPT", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/authorities/performanceMediums/context.json",
					"https://id.loc.gov/authorities/performanceMediums/activitystreams/feed/1.json"
					)),
			Map.entry("RBMS", new ActivityStreamsDatasetEntry(
					"https://id.loc.gov/vocabulary/rbmscv/context.json",
					"https://id.loc.gov/vocabulary/rbmscv/activitystreams/feed/1.json"
					))
		);

	public static ActivityStreamsDatasetEntry getParam(String type) {
		return DATASET_MAP.get(type);
	}
}
