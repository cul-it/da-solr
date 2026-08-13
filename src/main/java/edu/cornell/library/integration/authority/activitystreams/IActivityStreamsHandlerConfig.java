package edu.cornell.library.integration.authority.activitystreams;

import java.nio.file.Path;

public interface IActivityStreamsHandlerConfig {
	public String addedDate();
	public IFetcher fetcher();
	public ActivityStreamsDatasetEntry datasetEntry();

	record ActivityStreamsHandlerConfig(
		String addedDate,
		int chunkSize,
		String dataset
	) implements IActivityStreamsHandlerConfig {
		@Override
		public IFetcher fetcher() {
			return new IFetcher.HttpFetcher();
		}

		@Override
		public ActivityStreamsDatasetEntry datasetEntry() {
			return ActivityStreamsDataset.getParam(dataset);
		}
	}

	record ActivityStreamsHandlerConfigT(String addedDate, int chunkSize, String dataset, Path rootPath, String activityStreamsURL) implements IActivityStreamsHandlerConfig {
		@Override
		public IFetcher fetcher() {
			return new IFetcher.FileFetcher(rootPath);
		}

		@Override
		public ActivityStreamsDatasetEntry datasetEntry() {
			var ds = ActivityStreamsDataset.getParam(dataset);
			ds.url = activityStreamsURL;
			return ds;
		}
	}
}
