package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;

public class DataExportTest {

	public static void main(String[] args) throws IOException, InterruptedException, SQLException {
		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Current"));
		if (! config.isTestOkapiConfigured("Folio")) {
			System.out.println("Error: Test Okapi must be configured.");
			System.exit(1);
		}
		OkapiClient okapi = config.getOkapi("Folio");
		List<String> instances = new ArrayList<>();
		instances.add("a5098d03-f6b4-431d-bcf5-779f41390a72");
		instances.add("7c215d5e-de82-4f46-998d-791fe0b964b9");
		List<MarcRecord> records = DataExport.retrieveMarcByUuid(okapi, instances);
		try (Connection conn = config.getDatabaseConnection("Current");
			 PreparedStatement insert = conn.prepareStatement(
					 "REPLACE INTO generatedMarcFolio (instanceHrid, moddate, xml) VALUES (?, ?, ?)")) {
			for (MarcRecord r : records) {
				int fieldId = 0;
				for (DataField f : r.dataFields) if (f.tag.equals("999")) {fieldId = f.id; break;}
				r.dataFields.add(new DataField(fieldId - 1, "969",' ',' ',
						"‡a MARC RECORD DERIVED FROM FOLIO INSTANCE"));
				insert.setString(1, extractInstanceHrid(r));
				insert.setTimestamp(2, extractTimestamp(r));
				insert.setString(3, r.toXML(false));
				insert.executeUpdate();
			}
		}
	}

	private static String extractInstanceHrid(MarcRecord r) {
		for (ControlField f: r.controlFields) if (f.tag.equals("001"))
			return f.value;
		return null;
	}

	private static Timestamp extractTimestamp(MarcRecord r) {
		for (ControlField f: r.controlFields) if (f.tag.equals("005")) {
			Matcher m = dateMatcher.matcher(f.value);
			if (m.matches())
				return Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s.00000000",
						m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6)));
		}
		return null;
	}
	static Pattern dateMatcher = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2}).*");
}
