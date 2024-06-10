package edu.cornell.library.integration.authority;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class LookForNamesThatUsedToBeSubjects {

	public static void main(String[] args) throws SQLException {
		Config config = Config.loadConfig(Config.getRequiredArgsForDB("Authority"));
		try (Connection authority = config.getDatabaseConnection("Authority")) {
			List<String> changeFiles = getChangeFiles(authority);
			for (String file : changeFiles) {
				System.out.println(file);
				lookThroughUpdateFile(authority, file);
			}
		}
	}

	private static void lookThroughUpdateFile(Connection authority, String file) throws SQLException {
		try (PreparedStatement pstmt = authority.prepareStatement(
				"SELECT id, marc21 FROM authorityUpdate WHERE updateFile = ? ORDER BY positionInFile")) {
			pstmt.setString(1, file);
			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					String id = rs.getString(1);
					if (id.charAt(2) ==' ') continue;
					MarcRecord r = new MarcRecord(MarcRecord.RecordType.AUTHORITY,rs.getBytes(2));
					boolean usedToBeASubject = false;
					Subfield f010z = null;
					for (DataField f : r.dataFields) if (f.tag.equals("010"))
						for (Subfield sf : f.subfields) if (sf.code.equals('z'))
							if (sf.value.contains("sh")) {
								usedToBeASubject = true;
								f010z = sf;
							}
					if (! usedToBeASubject) continue;
					boolean isNowAName = false;
					DataField f1x0 = null;
					for (DataField f : r.dataFields) if (f.tag.startsWith("1"))
						if (f.tag.equals("100") || f.tag.equals("110")) {
							isNowAName = true;
							f1x0 = f;
						}
					if (! isNowAName) continue;
					
					System.out.printf("%s\t%s\t%s\n",id, f010z.value,f1x0.toString());
				}
			}
			
		}
	}

	private static List<String> getChangeFiles(Connection authority) throws SQLException {
		try (Statement stmt = authority.createStatement();
				ResultSet rs = stmt.executeQuery(
						"select distinct updateFile from authorityUpdate"
						+ " where updateFile between 'unname13.01' AND 'unname24.53'")) {
			List<String> files = new ArrayList<String>();
			while ( rs.next() ) files.add(rs.getString(1));
			return files;
		}
	}

}
