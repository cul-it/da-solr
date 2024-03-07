package edu.cornell.library.integration.hathitrust;

import static edu.cornell.library.integration.hathitrust.Utilities.identifyPrefixes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import edu.cornell.library.integration.utilities.Config;


public class BuildHathiFilesDiffReport {

	public static void main(String[] args) throws SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Hathi");
		Config config = Config.loadConfig(requiredArgs);

		Map<String, String> env = System.getenv();
		List<String> dbPrefixes = identifyPrefixes(env.get("prefixes"));
		System.out.println("Prefixes: '"+String.join("', '", dbPrefixes)+"'");
		if (dbPrefixes.size() != 2) {
			System.out.println("Exactly 2 prefixes must be specified for comparison.");
			System.exit(1);
		}
		confirmTablesExist(config, dbPrefixes);
		Set<String> ids = getAllVolumeIdentifiers(config, dbPrefixes);
		compareRecords(config, dbPrefixes, ids);
	}

	private static void compareRecords(Config config, List<String> prefixes, Set<String> ids) throws SQLException {
		String table1 = prefixes.get(0) + "raw_hathi";
		String table2 = prefixes.get(1) + "raw_hathi";
		try (Connection hathidb = config.getDatabaseConnection("Hathi");
				PreparedStatement stmt1 = hathidb.prepareStatement(
						String.format("SELECT * FROM %s WHERE Volume_Identifier = ?", table1));
				PreparedStatement stmt2 = hathidb.prepareStatement(
						String.format("SELECT * FROM %s WHERE Volume_Identifier = ?", table2));
				) {
			for (String id : ids) {
				stmt1.setString(1, id);
				stmt2.setString(1, id);
				try (ResultSet rs1 = stmt1.executeQuery(); ResultSet rs2 = stmt2.executeQuery()) {
					if (! rs1.next()) { System.out.printf("%s not in table %s\n", id, table1); continue; }
					if (! rs2.next()) { System.out.printf("%s not in table %s\n", id, table2); continue; }
					List<String> differences = new ArrayList<>();
					String[] stringFields = {"Access", "Rights","UofM_Record_Number","Enum_Chrono","Source",
							"Source_Inst_Record_Number","OCLC_Numbers","ISBNs","ISSNs","LCCNs","Title","Imprint",
							"Rights_determine_reason_code","Date_Last_Update","Pub_Date","Pub_Place","Language",
							"Bib_Format","Digitization_Agent_code","Content_provider_code","Responsible_Entity_code",
							"Collection_code","Access_profile","Author"};
					for (String field : stringFields) {
						String val1 = rs1.getString(field);
						String val2 = rs2.getString(field);
//						System.out.printf("%s %s : %s => %s\n", id, field, val1, val2);
						if (! Objects.equals(val1, val2))
							differences.add(String.format("%s: %s => %s", field, val1, val2));
					}
					String[] intFields = {"Gov_Doc"};
					for (String field : intFields) {
						Integer val1 = rs1.getInt(field);
						Integer val2 = rs2.getInt(field);
						if (! Objects.equals(val1, val2))
							differences.add(String.format("%s: %d => %d", field, val1, val2));
					}
					if ( ! differences.isEmpty() )
						System.out.printf("%s: [%s]\n", id, String.join("; ", differences));
				}
			}
		}
	}


	private static Set<String> getAllVolumeIdentifiers(Config config, List<String> prefixes) throws SQLException {
		Set<String> ids = new TreeSet<>();
		try (Connection hathidb = config.getDatabaseConnection("Hathi");
				Statement stmt = hathidb.createStatement()) {
			String table1 = prefixes.get(0) + "raw_hathi";
			String table2 = prefixes.get(0) + "raw_hathi";
			try (ResultSet rs = stmt.executeQuery(String.format("SELECT Volume_Identifier FROM %s", table1))) {
				while (rs.next()) ids.add(rs.getString(1));
			}
			try (ResultSet rs = stmt.executeQuery(String.format("SELECT Volume_Identifier FROM %s", table2))) {
				while (rs.next()) ids.add(rs.getString(1));
			}
			System.out.printf("%d total unique Volume_Identifiers\n", ids.size());
		}
		return ids;
	}

	private static void confirmTablesExist(Config config, List<String> prefixes) throws SQLException {
		try (Connection hathidb = config.getDatabaseConnection("Hathi");
				Statement stmt = hathidb.createStatement()) {
			Integer table1count = null;
			for (String prefix : prefixes) {
				String tableName = prefix + "raw_hathi";
				try (ResultSet rs = stmt.executeQuery(String.format("SELECT COUNT(*) FROM %s", tableName))){
					while (rs.next()) {
						int count = rs.getInt(1);
						System.out.printf("%s -> %d records\n", tableName, count);
						if (count == 0) {
							System.out.println("No point to a difference report with empty table(s)");
							System.exit(3);
						}
						if (table1count == null) table1count = count;
						else if (table1count == rs.getInt(1)) System.out.println("Record counts match");
						else System.out.println("Record counts do not match");
					}
				} catch (SQLSyntaxErrorException e) {
					System.out.println(e.getMessage());
					System.exit(2);
				}
			}
		}
	}
}
