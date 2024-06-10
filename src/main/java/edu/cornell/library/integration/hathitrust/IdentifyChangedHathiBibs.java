package edu.cornell.library.integration.hathitrust;

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.folio.DownloadMARC;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.AWSS3;
import edu.cornell.library.integration.utilities.Config;

public class IdentifyChangedHathiBibs {


	public static void main(String[] args) throws
	SQLException, IOException, InterruptedException, XMLStreamException {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForDB("Hathi"));
		Config config = Config.loadConfig(requiredArgs);
		Catalog.DownloadMARC marc = Catalog.getMarcDownloader(config);
		Timestamp folioGoLive = Timestamp.valueOf("2021-07-01 00:00:00");
		Timestamp cursor = Timestamp.valueOf("2024-03-13 00:00:00");
		try ( Connection inventory = config.getDatabaseConnection("Current");
				Statement stmt = inventory.createStatement();
				ResultSet rs = stmt.executeQuery(
						"SELECT current_to_date FROM updateCursor WHERE cursor_name = 'hathi_upd'")) {
			while (rs.next())
				cursor = rs.getTimestamp(1);
		}


		List<HathiVolume> hathiFiles = getHathifilesList(config);
		System.out.printf("%d volumes from COO in Hathifiles\n",hathiFiles.size());
		Set<String> instancesToSend = new HashSet<>();

		for (HathiVolume vol : hathiFiles) {
			Map<String,Timestamp> recentBibids = new HashMap<>();
			for (String bibid : vol.bibids) {
				Timestamp folioTimestamp = getFolioTimestamp(config,bibid);
				if ( folioTimestamp == null ) {
//					System.out.printf("%s: h(%s) NOT IN FOLIO CACHE\n",bibid,hathiTimestamp);
					continue;
				}
				if ( folioTimestamp.before(cursor) ) continue;
				recentBibids.put(bibid, folioTimestamp);
			}
			if (recentBibids.isEmpty()) { System.out.print('.'); continue; }

			try { Thread.sleep(300); } catch (InterruptedException e) { System.exit(1); } // don't pressure Zephir API
			MarcRecord zephirMarc = getZephirRecord( vol.volumeId );
			if (zephirMarc == null) {
				System.out.printf("Record for volume %s not found in Zephir.\n", vol.volumeId);
				continue;
			}
			String zephirBibid = getZephirBibid(zephirMarc);
			if (zephirBibid == null) {
				System.out.printf("Zephir marc for %s lacks 035 sdr-coo value.\n", vol.volumeId);
				continue;
			}
			if ( ! recentBibids.containsKey(zephirBibid)) continue;

			Timestamp folioTimestamp = recentBibids.get(zephirBibid);
			if ( folioTimestamp.before(folioGoLive) )
				System.out.println("Instance untouched since Folio probably has no 903 fields. "+zephirBibid);
			System.out.printf("%s:%s: h(%s) f(%s)\n", zephirBibid,vol.volumeId,vol.moddate,folioTimestamp);

			// If record has been changed locally since cursor, compare to Zephir record to see if significant
			MarcRecord folioRec = marc.getMarc(MarcRecord.RecordType.BIBLIOGRAPHIC,zephirBibid);
			boolean has903 = recordHasMatching903(folioRec, vol.volumeId);
			if ( ! has903 ) {
				System.out.printf("Folio bib %s lacks 903 field for %s\n", zephirBibid, vol.volumeId);
				continue;
			}
			boolean bibChanged = determineWhetherRelevantBibChanges(zephirBibid, folioRec, zephirMarc);
//			MarcRecord hathiMarc = getHathiRecord( vol.volumeId );
			if ( ! bibChanged ) continue;

			String instanceUUID = getInstanceUUID(config, zephirBibid);
			instancesToSend.add(instanceUUID);
			System.out.println("instance "+instanceUUID);
			if ( isShadowRecord(zephirMarc) )
				System.out.printf("Shadow record %s queued for update.\n",vol.volumeId);
		}
		System.out.printf("%d instances to update\n",instancesToSend.size());
		for (String s : instancesToSend) System.out.println(s);
		String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
		AWSS3.putS3Object(config,
				"hathi/input_test/change_detection/bibs_"+today+"_"+instancesToSend.size()+".txt",
				String.join("\n", instancesToSend));

	}

	private static boolean isShadowRecord(MarcRecord r) {
		for (DataField f : r.dataFields)
			if (f.tag.equals("COM")) return true;
		return false;
	}

	private static boolean recordHasMatching903(MarcRecord folioRec, String volumeId) {
		for (DataField f : folioRec.dataFields)
			if (f.tag.equals("903"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('n'))
						if (volumeId.endsWith(sf.value))
							return true;
		return false;
	}

	private static String getZephirBibid(MarcRecord zephirMarc) {
		for (DataField f : zephirMarc.dataFields)
			if (f.tag.equals("035"))
				for (Subfield sf : f.subfields)
					if (sf.code.equals('a'))
						if (sf.value.startsWith("sdr-coo")) {
							String zBibid = sf.value.substring(7);
							if (zBibid.startsWith(".")) zBibid = zBibid.substring(1);
							return zBibid;
						}
		return null;
	}

	private static boolean determineWhetherRelevantBibChanges( String bibId, MarcRecord f, MarcRecord z ) {
		MarcRecord coreF = filterMarc(f);
		MarcRecord coreZ = filterMarc(z);
		boolean changed = ! coreF.toString().equals(coreZ.toString());
		if (changed)
			System.out.printf("Bibliographic changes found %s\n%s%s\n",bibId, coreF.toString(),coreZ.toString());
		return changed;
	}

	private static MarcRecord filterMarc( MarcRecord before ) {
		MarcRecord after = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		after.leader = String.format("     %s     %s", before.leader.substring(5, 12), before.leader.substring(17));
		for( ControlField f : before.controlFields )
			if (f.tag.equals("008")) after.controlFields.add(f);
		Map<String,TreeMap<String,DataField>> fields = new TreeMap<>();
		for( DataField f: before.dataFields ) {
			char fieldBlock = f.tag.charAt(0);
			switch(fieldBlock) {
			case '0':
				if (f.tag.equals("035"))
					for (Subfield sf : f.subfields) if (sf.code.equals('a') && sf.value.contains("OCoLC")) {
						if (! fields.containsKey(f.tag))
							fields.put(f.tag, new TreeMap<>());
						fields.get(f.tag).put(f.toString(), f);
					}
				break;
			case '1':
			case '2':
			case '3':
			case '7':
				if (! fields.containsKey(f.tag))
					fields.put(f.tag, new TreeMap<>());
				fields.get(f.tag).put(f.toString(), f);
				break;
			case '5':
				if (f.tag.equals("538") && f.concatenateSpecificSubfields("a").equals("Mode of access: Internet."))
					break;
				if (! fields.containsKey(f.tag))
					fields.put(f.tag, new TreeMap<>());
				fields.get(f.tag).put(f.toString(), f);
				break;
			case '6':
				if (f.concatenateSpecificSubfields("2").contains("fast"))
					break;
				if (! fields.containsKey(f.tag))
					fields.put(f.tag, new TreeMap<>());
				fields.get(f.tag).put(f.toString(), f);
				break;
			}
		}
		int id = 3;
		for (Map<String,DataField> fieldMap : fields.values())
			for (DataField f : fieldMap.values())
				after.dataFields.add(new DataField(++id,f.tag,f.ind1,f.ind2,f.subfields));
		return after;
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
	private static MarcRecord getZephirRecord(String volumeId) throws IOException {
		URL link = new URL("http://zephir.cdlib.org/api/item/"+volumeId+".json");
		HttpURLConnection httpURLConnection = (HttpURLConnection) link.openConnection();
		httpURLConnection.setRequestMethod("GET");
		int responseCode = httpURLConnection.getResponseCode();
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
		System.out.println("GET request failed");
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

	private static String getInstanceUUID(Config config, String bibid) throws SQLException {

		try (Connection inventory = config.getDatabaseConnection("Current");
				PreparedStatement stmt = inventory.prepareStatement(
						"SELECT id FROM instanceFolio WHERE hrid = ?")) {
			stmt.setString(1, bibid);
			try( ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) return rs.getString(1);
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
				" ORDER BY RAND()")) {
			while (rs.next()) {
				Timestamp moddate ;
				{	String s = rs.getString("Date_Last_Update");
					if ( s.isEmpty() ) moddate = timestampFromFileName(rs.getString("update_file_name"));
					else moddate = Timestamp.valueOf(s);
				}
				dates.add(new HathiVolume(rs.getString("Source_Inst_Record_Number").split(","),
						moddate,rs.getString("Volume_Identifier")));
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
		final String[] bibids;
		final Timestamp moddate;
		final String volumeId;

		public HathiVolume(String[] bibids, Timestamp moddate, String volumeId) {
			this.bibids = bibids;
			this.moddate = moddate;
			this.volumeId = volumeId;
		}

	}
}
