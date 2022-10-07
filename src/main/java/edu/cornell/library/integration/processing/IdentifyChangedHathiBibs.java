package edu.cornell.library.integration.processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.folio.DownloadMARC;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyChangedHathiBibs {


	public static void main(String[] args) throws
	SQLException, IOException, InterruptedException, XMLStreamException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Hathi"));
		Config config = Config.loadConfig(requiredArgs);
		Catalog.DownloadMARC marc = Catalog.getMarcDownloader(config);
		Timestamp folioGoLive = Timestamp.valueOf("2021-07-01 00:00:00");

		List<HathiVolume> hathiFiles = getHathifilesList(config);
		System.out.printf("%d volumes have moddates in Hathi\n",hathiFiles.size());
		Map<String,Timestamp> changedTimestamps = new HashMap<>();

		for (HathiVolume vol : hathiFiles) {
			Timestamp folioTimestamp = getFolioTimestamp(config,vol.bibid);
			if ( folioTimestamp == null ) {
//				System.out.printf("%s: h(%s) NOT IN FOLIO CACHE\n",bibid,hathiTimestamp);
				continue;
			}
			Timestamp hathiTimestamp = vol.moddate;
			if ( ! folioTimestamp.after(hathiTimestamp) ) continue;

			MarcRecord folioRec = null;
			// If the Folio timestamp is pre Folio go-live, the modification date will not represent
			// an actual modification (beyond what's involved in Folio import (999)).
			// MARC 005 will represent the last mod date in Voyager.
			if ( folioTimestamp.before(folioGoLive) ) {
				try (Connection inventory = config.getDatabaseConnection("Current")) {
					System.out.printf("%s: old f(%s) date\n", vol.bibid,folioTimestamp);
					folioRec = marc.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC, vol.bibid);
					Timestamp folio005timestamp = null;
					for ( ControlField f : folioRec.controlFields ) if ( f.tag.equals("005") ) {
						Matcher m = dateMatcher.matcher(f.value);
						if ( m.matches() ) {
							String timestamp = String.format("%s-%s-%s %s:%s:%s",
									m.group(1),m.group(2),m.group(3),m.group(4),m.group(5),m.group(6));
							folio005timestamp = Timestamp.valueOf(timestamp);
						}
					}
					System.out.printf("%s: h(%s) f005(%s)\n", vol.bibid,hathiTimestamp,folio005timestamp);
					if ( folio005timestamp != null && folio005timestamp.after(hathiTimestamp) ) {
						System.out.printf("%s: h(%s) f005(%s) CHANGED IN VOYAGER\n",
								vol.bibid,hathiTimestamp,folio005timestamp);
						changedTimestamps.put(vol.bibid, folio005timestamp);
					} else continue;
				}
			} else {
				System.out.printf("%s:%s: h(%s) f(%s) CHANGED IN FOLIO\n",
						vol.bibid,vol.volumeId,hathiTimestamp,folioTimestamp);
				changedTimestamps.put(vol.bibid, folioTimestamp);
			}

			// If we get to this point, it should mean the bib has been changed locally.
			// Next, compare to Zephyr and/or HT record to see if changes matter
			if (folioRec == null) folioRec = marc.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC,vol.bibid);
			System.out.println(folioRec.toString());
			MarcRecord zephyrMarc = getZephyrRecord( vol.volumeId );
			System.out.println(zephyrMarc.toString());
			MarcRecord hathiMarc = getHathiRecord( vol.volumeId );
			System.out.println(hathiMarc.toString());
		}
		System.out.printf("%d changed bibs\n",changedTimestamps.size());

	}
	private static MarcRecord getHathiRecord(String volumeId) throws IOException, XMLStreamException {
		URL link = new URL("https://catalog.hathitrust.org/api/volumes/full/htid/"+volumeId+".json");
		HttpURLConnection httpURLConnection = (HttpURLConnection) link.openConnection();
		httpURLConnection.setRequestMethod("GET");
		int responseCode = httpURLConnection.getResponseCode();
		System.out.println("GET Response Code :: " + responseCode);
		if (responseCode == HttpURLConnection.HTTP_OK) {
			try ( BufferedReader in = new BufferedReader(
					new InputStreamReader(httpURLConnection.getInputStream()));){
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in .readLine()) != null) {
					response.append(inputLine);
				}
				Map<String,Object> json = mapper.readValue(response.toString(), Map.class);
				Map<String,Object> records = Map.class.cast(json.get("records"));
				for (String key : records.keySet()) System.out.println(key);
				String record = (String)Map.class.cast(
						records.get(records.keySet().iterator().next())).get("marc-xml");
				return new MarcRecord(RecordType.BIBLIOGRAPHIC,record,false);
			}

		}
		System.out.println("GET request not worked");
		return null;
	}
	private static MarcRecord getZephyrRecord(String volumeId) throws IOException {
		URL link = new URL("http://zephir.cdlib.org/api/item/"+volumeId+".json");
		HttpURLConnection httpURLConnection = (HttpURLConnection) link.openConnection();
		httpURLConnection.setRequestMethod("GET");
		int responseCode = httpURLConnection.getResponseCode();
		System.out.println("GET Response Code :: " + responseCode);
		if (responseCode == HttpURLConnection.HTTP_OK) {
			try ( BufferedReader in = new BufferedReader(
					new InputStreamReader(httpURLConnection.getInputStream()));){
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in .readLine()) != null) {
					response.append(inputLine);
				}
				return DownloadMARC.jsonMarcToMarcRecord(
						mapper.readValue(response.toString(), Map.class));
			}

		}
		System.out.println("GET request not worked");
		return null;
	}
	static Pattern dateMatcher = Pattern.compile("(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})(\\d{2}).*");
	private static ObjectMapper mapper = new ObjectMapper();

	private static Timestamp getFolioTimestamp(Config config, String bibid) throws SQLException {

		try (Connection inventory = config.getDatabaseConnection("Current");
				PreparedStatement stmt = inventory.prepareStatement(
						"SELECT moddate FROM bibFolio WHERE instanceHrid = ?")) {
			stmt.setString(1, bibid);
			try( ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) return rs.getTimestamp(1);
			}
			
		}
		return null;
	}

	private static List<HathiVolume> getHathifilesList(Config config) throws SQLException {

		List<HathiVolume> dates = new ArrayList<>();
		try (Connection hathidb = config.getDatabaseConnection("Hathi");
				Statement stmt = hathidb.createStatement();
				ResultSet rs = stmt.executeQuery(
				"SELECT Source_Inst_Record_Number, Date_Last_Update, Volume_Identifier, update_file_name"+
				"  FROM raw_hathi"+
				" WHERE source = 'COO'"+
				" ORDER BY RAND() LIMIT 10")) {
			while (rs.next()) {
				Timestamp moddate ;
				{	String s = rs.getString("Date_Last_Update");
					if ( s.isEmpty() ) moddate = timestampFromFileName(rs.getString("update_file_name"));
					else moddate = Timestamp.valueOf(s);
				}
				for (String bibid : rs.getString("Source_Inst_Record_Number").split(","))
					dates.add(new HathiVolume(bibid,moddate,rs.getString("Volume_Identifier")));
			}
		}
		return dates;
	}

	private static Timestamp timestampFromFileName(String filename) {
		System.out.println(filename);
		Matcher m = yyyymmdd.matcher(filename);
		if ( m.find() )
			return Timestamp.valueOf(
					String.format("%s-%s-%s 00:00:00",m.group(1),m.group(2),m.group(3)));
		return null;
	}
	private static Pattern yyyymmdd = Pattern.compile("^.*(\\d{4})(\\d{2})(\\d{2}).txt$");

	private static class HathiVolume {
		final String bibid;
		final Timestamp moddate;
		final String volumeId;

		public HathiVolume(String bibid, Timestamp moddate, String volumeId) {
			this.bibid = bibid;
			this.moddate = moddate;
			this.volumeId = volumeId;
		}

	}
}
