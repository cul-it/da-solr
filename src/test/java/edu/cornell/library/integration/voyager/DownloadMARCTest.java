package edu.cornell.library.integration.voyager;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class DownloadMARCTest {

	static Catalog.DownloadMARC genericClassDownloader = null;
	static DownloadMARC specificClassDownloader = null;

	@BeforeClass
	public static void setup()  {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Voy");
		Config config = Config.loadConfig(requiredArgs);
		config.setCatalogClass("edu.cornell.library.integration.voyager");
		genericClassDownloader = Catalog.getMarcDownloader(config);
		specificClassDownloader = new DownloadMARC();
		specificClassDownloader.setConfig(config);
	}

	@Test
	public void downloadBib10759251() throws SQLException, IOException, InterruptedException {
		MarcRecord rec = genericClassDownloader.getMarc(RecordType.BIBLIOGRAPHIC, 10759251);
		assertTrue(rec.toString().contains("\n001    10759251\n"));
		rec = specificClassDownloader.getMarc(RecordType.BIBLIOGRAPHIC, 10759251);
		assertTrue(rec.toString().contains("\n001    10759251\n"));
		byte[] recordBytes = specificClassDownloader.downloadMrc(RecordType.BIBLIOGRAPHIC, 10759251);
		assertTrue((new String(recordBytes)).contains("10759251"));
	}
}
