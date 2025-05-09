package edu.cornell.library.integration.hathitrust;

import static edu.cornell.library.integration.hathitrust.Utilities.identifyPrefixes;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import edu.cornell.library.integration.utilities.Config;

public class BuildLocalHathiFilesDB {
	/**
	 *  BuildLocalHathiFilesDB<br/>
	 *  Download most recent full export of hathifiles database and all intervening intermitent updates
	 *  from HathiTrust and use them to build a local MySql database. The main table, raw_hathi, and 
	 *  supplementary tables volume_to_source_inst_rec_num and volume_to_oclc will be given the table
	 *  name prefix provided in the environment variable "prefixes".<br/>
	 *  <br/>
	 *  The raw data coming from HT is in gzipped tab-delimited files.
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, SQLException, InterruptedException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Hathi");
		requiredArgs.add("hathifilesUrl");
		Config config = Config.loadConfig(requiredArgs);
		String hathifilesUrl = config.getHathifilesUrl();
		Map<String, String> env = System.getenv();
		List<String> dbPrefixes = identifyPrefixes(env.get("prefixes"));
		System.out.println("Prefixes: '"+String.join("', '", dbPrefixes)+"'");
		ensureTablesExist(config, dbPrefixes);

		// INCREMENTAL DATABASE UPDATE
		String dayParam = env.get("day");
		if ( dayParam != null ) {
			String filename = null;
			if (dayParam.equals("yesterday")) {
				Date yesterday = new Date(System.currentTimeMillis()-(24*60*60*1000));//now() minus 1 day in milliseconds
				filename = yesterdayHathiFilenameFormat.format(yesterday);
			} else {
				Matcher m = dateMatcher.matcher(dayParam);
				if ( ! m.matches() ) {
					System.out.printf("Day paramter '%s' does not match expected format.\n",dayParam);
					System.exit(1);
				}
				filename = String.format(incrementalHathiFilenameFormat, dayParam);
			}
			try ( Connection hathidb = config.getDatabaseConnection("Hathi") ) {
				loadFileWithRetries(hathidb,hathifilesUrl,filename,dbPrefixes);
			}
			System.exit(0);
		}

		// FULL DATABASE BUILD
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
			confirmDbPresentAndEmpty(hathidb, dbPrefixes);
			for (String fileToLoad : filesToLoad)
				loadFileWithRetries(hathidb,hathifilesUrl,fileToLoad,dbPrefixes);
		}

	}

	private static void ensureTablesExist(Config config, List<String> prefixes) throws SQLException {
		try ( Connection hathidb = config.getDatabaseConnection("Hathi");
				Statement stmt = hathidb.createStatement()) {

			Set<String> tables = new HashSet<>();
			try (ResultSet rs = stmt.executeQuery("SHOW TABLES") ) {
				while (rs.next()) tables.add(rs.getString(1));
			}

			for (String prefix : prefixes) {
				if ( ! tables.contains(prefix+"raw_hathi")) {
					stmt.executeUpdate(
					"CREATE TABLE `"+prefix+"raw_hathi` ("
					+ "  `Volume_Identifier` varchar(128) NOT NULL DEFAULT '',"
					+ "  `Access` text DEFAULT NULL,"
					+ "  `Rights` text DEFAULT NULL,\r\n"
					+ "  `UofM_Record_Number` varchar(128) DEFAULT NULL,"
					+ "  `Enum_Chrono` text DEFAULT NULL,"
					+ "  `Source` varchar(12) DEFAULT NULL,\r\n"
					+ "  `Source_Inst_Record_Number` varchar(10000) DEFAULT NULL,"
					+ "  `OCLC_Numbers` text DEFAULT NULL,"
					+ "  `ISBNs` text DEFAULT NULL,"
					+ "  `ISSNs` text DEFAULT NULL,"
					+ "  `LCCNs` text DEFAULT NULL,"
					+ "  `Title` text DEFAULT NULL,"
					+ "  `Imprint` text DEFAULT NULL,"
					+ "  `Rights_determine_reason_code` varchar(8) DEFAULT NULL,"
					+ "  `Date_Last_Update` varchar(24) DEFAULT NULL,"
					+ "  `Gov_Doc` int(1) DEFAULT NULL,"
					+ "  `Pub_Date` varchar(16) DEFAULT NULL,"
					+ "  `Pub_Place` varchar(128) DEFAULT NULL,"
					+ "  `Language` varchar(128) DEFAULT NULL,"
					+ "  `Bib_Format` varchar(16) DEFAULT NULL,"
					+ "  `Digitization_Agent_code` varchar(128) DEFAULT NULL,"
					+ "  `Content_provider_code` varchar(128) DEFAULT NULL,"
					+ "  `Responsible_Entity_code` varchar(128) DEFAULT NULL,"
					+ "  `Collection_code` varchar(128) DEFAULT NULL,"
					+ "  `Access_profile` varchar(512) DEFAULT NULL,"
					+ "  `Author` varchar(2500) DEFAULT NULL,"
					+ "  `update_file_name` varchar(128) DEFAULT NULL,"
					+ "  PRIMARY KEY (`Volume_Identifier`),\r\n"
					+ "  KEY `UofM_Record_Number` (`UofM_Record_Number`),"
					+ "  KEY `Author` (`Author`(250)),"
					+ "  KEY `Access_profile` (`Access_profile`(250)),"
					+ "  KEY `Local_Identifiers` (`Source`,`Source_Inst_Record_Number`(12)),"
					+ "  KEY `Source_Inst_Record_Number_idx` (`Source_Inst_Record_Number`(250))"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci");
				}
				if ( ! tables.contains(prefix+"volume_to_oclc")) {
					stmt.executeUpdate(
					"CREATE TABLE `"+prefix+"volume_to_oclc` ("
					+ "  `Volume_Identifier` varchar(128) DEFAULT NULL,"
					+ "  `OCLC_Number` varchar(250) DEFAULT NULL,"
					+ "  KEY `Volume_Identifier` (`Volume_Identifier`),"
					+ "  KEY `OCLC_Number` (`OCLC_Number`)"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci");
				}
				if ( ! tables.contains(prefix+"volume_to_source_inst_rec_num")) {
					stmt.executeUpdate(
					"CREATE TABLE `"+prefix+"volume_to_source_inst_rec_num` ("
					+ "  `Volume_Identifier` varchar(128) DEFAULT NULL,"
					+ "  `Source_Inst_Record_Number` varchar(256) DEFAULT NULL,"
					+ "  KEY `Volume_Identifier` (`Volume_Identifier`),"
					+ "  KEY `Source_Inst_Record_Number` (`Source_Inst_Record_Number`)"
					+ ") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci");
				}
			}
		}
	}

	private static void confirmDbPresentAndEmpty(Connection hathidb, List<String> prefixes) throws SQLException {
		for (String prefix : prefixes)
		try (Statement stmt = hathidb.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM "+prefix+"raw_hathi");) {
			while ( rs.next() )
				if (rs.getInt(1) > 0 ) {
					System.out.println("Can't do full database reload on populated database. Point config at a new db space.");
					System.exit(2);
				}
		}
	}

	private static SimpleDateFormat fullHathiFilenameFormat = new SimpleDateFormat("'hathi_full_'yyyyMM'01.txt.gz'");
	private static SimpleDateFormat updateHathiFilenameFormat = new SimpleDateFormat("'hathi_upd_'yyyyMM'%02d.txt.gz'");
	private static SimpleDateFormat yesterdayHathiFilenameFormat = new SimpleDateFormat("'hathi_upd_'yyyyMMdd'.txt.gz'");
	private static String incrementalHathiFilenameFormat = "hathi_upd_%s.txt.gz";
	static Pattern dateMatcher = Pattern.compile("\\d{8}");

	public static void loadFileWithRetries(Connection hathidb, String url, String filename, List<String> prefixes)
			throws IOException, SQLException, InterruptedException {
		int[] startCount = {0};
		int attemptLimit = 20;
		boolean done = false;
		for (int attempt = 1; !done && attempt <= attemptLimit; attempt++)  
		try {
			if (attempt > 1) {
				System.out.format("Attempt #%d of %d\n",attempt,attemptLimit);
			}
			loadFile(hathidb, url, filename, prefixes, startCount);
			done = true;
		} catch (EOFException|SocketException e) {
			LOGGER.log(Level.WARNING, e.getClass()+" "+ e.getMessage());
		} catch (FileNotFoundException e) {
			if (attempt < attemptLimit) {
				LOGGER.log(Level.WARNING, e.getClass()+" "+ e.getMessage()+
						"\nFile not found can be a glitch. Wait 10 seconds and see" );
				Thread.sleep(10_000);
			} else
				LOGGER.log(Level.WARNING, e.getClass()+" "+ e.getMessage());
		}
		if ( ! done ) {
			LOGGER.log(Level.SEVERE, "Data not loaded after maximum retries.");
			throw new IOException("Data not loaded after maximum retries.");
		}
	}

	public static void loadFile(Connection hathidb, String url, String filename, List<String> prefixes, int[] startCount)
			throws IOException, SQLException {
		URL website = new URL(url+filename);
		boolean isInitialLoad = filename.startsWith("hathi_full");
		System.out.printf("Loading file %s%s\n", filename, isInitialLoad?" (db initialize)":"");
		int start = startCount[0];
		if (start > 0)
			System.out.format("Resuming from after row %d\n", start);
		try (InputStream is = website.openStream();
			 GZIPInputStream gzis = new GZIPInputStream(is);
			 InputStreamReader reader = new InputStreamReader(gzis);
			 BufferedReader in = new BufferedReader(reader) ) {
			List<PreparedStatement> insertStmts = new ArrayList<>();
			List<PreparedStatement> deleteOclcStmts = new ArrayList<>();
			List<PreparedStatement> insertOclcStmts = new ArrayList<>();
			List<PreparedStatement> deleteSourceNoStmts = new ArrayList<>();
			List<PreparedStatement> insertSourceNoStmts = new ArrayList<>();
			for (String prefix : prefixes) {
				insertStmts.add(hathidb.prepareStatement(
					"REPLACE INTO "+prefix+"raw_hathi VALUES ("
					+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
					+ "?, ?, ?, ?, ?, ?, ?)"));
				deleteOclcStmts.add(hathidb.prepareStatement(
						"DELETE FROM "+prefix+"volume_to_oclc WHERE Volume_Identifier = ?"));
				insertOclcStmts.add(hathidb.prepareStatement(
						"INSERT INTO "+prefix+"volume_to_oclc VALUES (? , ?)"));
				deleteSourceNoStmts.add(hathidb.prepareStatement(
						"DELETE FROM "+prefix+"volume_to_source_inst_rec_num WHERE Volume_Identifier = ?"));
				insertSourceNoStmts.add(hathidb.prepareStatement(
						"INSERT INTO "+prefix+"volume_to_source_inst_rec_num VALUES (? , ?)"));
			}
			List<PreparedStatement> allStmts = new ArrayList<>();
			allStmts.addAll(insertStmts);
			allStmts.addAll(deleteOclcStmts);
			allStmts.addAll(insertOclcStmts);
			allStmts.addAll(deleteSourceNoStmts);
			allStmts.addAll(insertSourceNoStmts);
			String line;
			int count = 0;
			int batchSize = 50_000;
			while ((line = in.readLine()) != null) {

				// fast forward on resume
				if (count < start) { count++; continue; }

				String[] columns = line.split("\\t");
				String Volume_Identifier = columns[0];
				if ( columns.length < 25 )
					System.out.printf("%s (%d) %d\n", Volume_Identifier,columns.length, count);
				String Source_Inst_Record_Number = dedupeList(columns[6]);
				String OCLC_Numbers = columns[7];
				String Access_profile = (columns.length > 24)?columns[24]:"";
				String Author = (columns.length > 25)?columns[25]:null;

				for (PreparedStatement insert : insertStmts) {
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
				insert.setString(25,Access_profile);// Access_profile
				insert.setString(26,Author);// Author
				insert.setString(27,filename);// update_file_name
				insert.addBatch();
				}

				if ( ! isInitialLoad) for (PreparedStatement deleteOclc : deleteOclcStmts) {
				deleteOclc.setString(1, Volume_Identifier);
				deleteOclc.addBatch();;
				}
				String[] oclcs = OCLC_Numbers.split(", *");
				for (String oclc : oclcs) {
					if ( oclc.isBlank() ) continue;
					for (PreparedStatement insertOclc : insertOclcStmts) {
					insertOclc.setString(1, Volume_Identifier);
					insertOclc.setString(2, oclc.trim());
					insertOclc.addBatch();
					}
				}

				if ( ! isInitialLoad) for (PreparedStatement deleteSourceNo : deleteSourceNoStmts) {
				deleteSourceNo.setString(1, Volume_Identifier);
				deleteSourceNo.addBatch();
				}
				String[] sourcenos = Source_Inst_Record_Number.split(",");
				for (String sourceno : sourcenos) {
					if ( sourceno.isBlank() ) continue;
					for (PreparedStatement insertSourceNo : insertSourceNoStmts) {
					insertSourceNo.setString(1, Volume_Identifier);
					insertSourceNo.setString(2, sourceno.trim());
					insertSourceNo.addBatch();
					}
				}
				count++;
				if (count % batchSize == 0) {
					executeSqlBatchStmts(count, allStmts);
					startCount[0] = count;

//					Random rand = new Random();
//					switch (rand.nextInt(4)) {
//					case 0:throw new EOFException("artificially thrown exception");
//					case 1:throw new SocketException("artificially thrown exception");
//					}
				}
			}
			if (count % batchSize > 0)
				executeSqlBatchStmts(count, allStmts);
			for (PreparedStatement s : allStmts) s.close();
			System.out.printf("%d bibs loaded from file %s\n", count,filename);
		}
	}

	private static void executeSqlBatchStmts(int count, List<PreparedStatement> allStatements) throws SQLException {
		List<String> resultCounts = new ArrayList<>();
		for (PreparedStatement pstmt : allStatements) {
			int[] results = pstmt.executeBatch();
			resultCounts.add(commify(results.length)); //counting executions, not affected recs
		}
		System.out.format("Loaded records (%s) - up to position %s\n", String.join(", ", resultCounts),commify(count));
	}

	private static String commify (int number) {
		String s = String.valueOf(number);
		if (s.length() <= 3) return s;
		List<String> parts = new ArrayList<>();
		int i = 0;
//		System.out.println(s);
		int remainder = s.length() % 3;
		if (remainder > 0) {
			parts.add(s.substring(0, remainder));
			i = remainder;
		}
		while (i < s.length() - 1) {
			parts.add(s.substring(i, i+3)); i += 3; }
		return String.join(",", parts);
	}
	private static String dedupeList(String list) {
		Set<String> after = new LinkedHashSet<>();
		for (String s : list.split(", *")) after.add(s);
		return String.join(",",after);
	}
	private static final Logger LOGGER = Logger.getLogger( "edu.cornell.library.integration.hathitrust.BuildLocalHathiFilesDB" );
}
