package edu.cornell.library.integration.authority.activitystreams;

import java.nio.file.Path;

import edu.cornell.library.integration.authority.activitystreams.IFetcher.FileFetcher;
import edu.cornell.library.integration.authority.activitystreams.IFetcher.HttpFetcher;

public interface IHandlerConfig {
	public String addedDate();
	public IFetcher fetcher();
	public DatasetEntry datasetEntry();

	record HandlerConfig(
		String addedDate,
		int chunkSize,
		String dataset
	) implements IHandlerConfig {
		@Override
		public IFetcher fetcher() {
			return new IFetcher.HttpFetcher();
		}

		@Override
		public DatasetEntry datasetEntry() {
			return Dataset.getParam(dataset);
		}
	}

	record HandlerConfigT(String addedDate, int chunkSize, String dataset, Path rootPath, String activityStreamsURL) implements IHandlerConfig {
		@Override
		public IFetcher fetcher() {
			HttpFetcher httpFetcher = new IFetcher.HttpFetcher();
			FileFetcher fileFetcher = new IFetcher.FileFetcher(rootPath);
			return new IFetcher.TestFetcher(httpFetcher, fileFetcher);
		}

		@Override
		public DatasetEntry datasetEntry() {
			var ds = Dataset.getParam(dataset);
			ds.url = activityStreamsURL;
			return ds;
		}
	}
}
