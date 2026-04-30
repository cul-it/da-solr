package edu.cornell.library.integration.authority.activitystreams;

public class ActivityStreamsParamsEntry {
	public String contextUrl;
	public String idPrefix;
	public String url;

	public ActivityStreamsParamsEntry(String contextUrl, String idPrefix, String url) {
		this.contextUrl = contextUrl;
		this.idPrefix = idPrefix;
		this.url = url;
	}
}
