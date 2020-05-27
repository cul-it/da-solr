package edu.cornell.library.integration.catalog;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class Catalog {

	public static DownloadMARC getMarcDownloader( Config config )
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		Class c = Class.forName(config.getCatalogClass()+".DownloadMARC");
		DownloadMARC downloader = (DownloadMARC) c.newInstance();
		downloader.setConfig(config);
		return downloader;
	}

	public interface DownloadMARC {
		public MarcRecord getMarc( RecordType type, Integer id ) throws SQLException, IOException, InterruptedException;
		public void setConfig( Config config );
		public List<MarcRecord> retrieveRecordsByIdRange (RecordType type, Integer from, Integer to)
				throws SQLException, IOException;
	}

}
