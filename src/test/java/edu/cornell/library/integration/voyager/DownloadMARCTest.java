package edu.cornell.library.integration.voyager;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class DownloadMARCTest {

	static DownloadMARC download = null;

	@BeforeClass
	public static void setup() {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Voy");
		Config config = Config.loadConfig(requiredArgs);
		download = new DownloadMARC(config);
		System.out.println("Hello");
	}

	@Test
	public void downloadBib10759251() throws SQLException, IOException, InterruptedException {
		byte[] bytes = download.downloadMrc(RecordType.BIBLIOGRAPHIC, 10759251);
		System.out.println(new String(bytes));
		MarcRecord rec = new MarcRecord(RecordType.BIBLIOGRAPHIC,bytes);
		System.out.println(rec.toString());
		System.out.println(rec.toXML());
	}
}
