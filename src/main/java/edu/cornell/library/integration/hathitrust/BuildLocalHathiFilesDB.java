package edu.cornell.library.integration.hathitrust;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import edu.cornell.library.integration.utilities.Config;

public class BuildLocalHathiFilesDB {

	public static void main(String[] args) throws IOException, SQLException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Hathi");
		requiredArgs.add("hathifilesUrl");
		Config config = Config.loadConfig(requiredArgs);
		String hathifilesUrl = config.getHathifilesUrl();
		List<String> filesToLoad = new ArrayList<>();

		Date today = new Date();
		Calendar rightNow = Calendar.getInstance();
		String fullFile = fullHathiFilenameFormat.format(today);
		filesToLoad.add(fullFile);
		int dayOfMonth = rightNow.get(Calendar.DAY_OF_MONTH);
		if ( dayOfMonth > 2 ) {
			String updateFilePattern = updateHathiFilenameFormat.format(today);
			for (int day = 2; day < dayOfMonth; day++ ) {
				String updateFile = String.format(updateFilePattern, day);
				filesToLoad.add(updateFile);
			}
		}
		System.out.printf("%d files to load: [%s]\n", filesToLoad.size(), String.join(", ", filesToLoad));

		try ( Connection hathidb = config.getDatabaseConnection("Hathi") ) {
			for (String fileToLoad : filesToLoad)
				loadFile(hathidb,hathifilesUrl,fileToLoad);
		}

	}

	private static SimpleDateFormat fullHathiFilenameFormat = new SimpleDateFormat("'hathi_full_'yyyyMM'01.txt.gz'");
	private static SimpleDateFormat updateHathiFilenameFormat = new SimpleDateFormat("'hathi_upd_'yyyyMM'%02d.txt.gz'");

	public static void loadFile(Connection hathidb, String url, String filename) throws IOException, SQLException {
		URL website = new URL(url+filename);
		System.out.printf("Loading file %s\n", filename);
		try (InputStream is = website.openStream();
			 GZIPInputStream gzis = new GZIPInputStream(is);
			 InputStreamReader reader = new InputStreamReader(gzis);
			 BufferedReader in = new BufferedReader(reader);
			 PreparedStatement insert = hathidb.prepareStatement(
					"REPLACE INTO raw_hathi VALUES ("
					+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, ?)");
			 PreparedStatement deleteOclc = hathidb.prepareStatement(
					"DELETE FROM volume_to_oclc WHERE Volume_Identifier = ?");
			 PreparedStatement insertOclc = hathidb.prepareStatement(
					"INSERT INTO volume_to_oclc VALUES (? , ?)");
			 PreparedStatement deleteSourceNo = hathidb.prepareStatement(
					"DELETE FROM volume_to_source_inst_rec_num WHERE Volume_Identifier = ?");
			 PreparedStatement insertSourceNo = hathidb.prepareStatement(
					"INSERT INTO volume_to_source_inst_rec_num VALUES (? , ?)"); ) {

			String line;
			int count = 0;
			while ((line = in.readLine()) != null) {
				String[] columns = line.split("\\t");
				String Volume_Identifier = columns[0];
				String Source_Inst_Record_Number = dedupeList(columns[6]);
				String OCLC_Numbers = columns[7];
				String Author = (columns.length > 25)?columns[25]:null;

				insert.setString(1, Volume_Identifier);
				insert.setString(2, columns[ 1]);// Access
				insert.setString(3, columns[ 2]);// Rights
				insert.setString(4, columns[ 3]);// UofM_Record_Number
				insert.setString(5, columns[ 4]);// Enum_Chrono
				insert.setString(6, columns[ 5]);// Source
				insert.setString(7, Source_Inst_Record_Number);
				insert.setString(8, OCLC_Numbers);
				insert.setString(9, columns[ 8]);// ISBNs
				insert.setString(10,columns[ 9]);// ISSNs
				insert.setString(11,columns[10]);// LCCNs
				insert.setString(12,columns[11]);// Title
				insert.setString(13,columns[12]);// Imprint
				insert.setString(14,columns[13]);// Rights_determine_reason_code
				insert.setString(15,columns[14]);// Date_Last_Update
				insert.setBoolean(16, columns[15].equals("1"));// Gov_Doc
				insert.setString(17,columns[16]);// Pub_Date
				insert.setString(18,columns[17]);// Pub_Place
				insert.setString(19,columns[18]);// Language
				insert.setString(20,columns[19]);// Bib_Format
				insert.setString(21,columns[20]);// Digitization_Agent_code
				insert.setString(22,columns[21]);// Content_provider_code
				insert.setString(23,columns[22]);// Responsible_Entity_code
				insert.setString(24,columns[23]);// Collection_code
				insert.setString(25,columns[24]);// Access_profile
				insert.setString(26,Author);// Author
				insert.setString(27,filename);// update_file_name
				insert.executeUpdate();

				deleteOclc.setString(1, Volume_Identifier);
				deleteOclc.executeUpdate();
				String[] oclcs = OCLC_Numbers.split(", *");
				for (String oclc : oclcs) {
					if ( oclc.isBlank() ) continue;
					insertOclc.setString(1, Volume_Identifier);
					insertOclc.setString(2, oclc.trim());
					insertOclc.executeUpdate();
				}

				deleteSourceNo.setString(1, Volume_Identifier);
				deleteSourceNo.executeUpdate();
				String[] sourcenos = Source_Inst_Record_Number.split(",");
				for (String sourceno : sourcenos) {
					if ( sourceno.isBlank() ) continue;
					insertSourceNo.setString(1, Volume_Identifier);
					insertSourceNo.setString(2, sourceno.trim());
					insertSourceNo.executeUpdate();
				}
			}
			System.out.printf("%d bibs loaded from file %s\n", count,filename);
		}
	}

	private static String dedupeList(String list) {
		Set<String> after = new LinkedHashSet<>();
		for (String s : list.split(", *")) after.add(s);
		return String.join(",",after);
	}
}
