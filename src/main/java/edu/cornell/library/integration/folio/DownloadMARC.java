package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.Normalizer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.catalog.Catalog;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class DownloadMARC implements Catalog.DownloadMARC {
	private Config config;

	@Override public void setConfig(Config config) { this.config = config; }

	@SuppressWarnings("resource")
	@Override
	public MarcRecord getMarc(RecordType type, String id) throws SQLException, IOException, InterruptedException {
		if (! type.equals(RecordType.BIBLIOGRAPHIC) )
			throw new IllegalArgumentException(
					String.format("Folio only contains Bibliographic MARC. Request for (%s, %s) invalid\n",type,id));
		Connection inventory = this.config.getDatabaseConnection("Current");
		String instanceId = null;
		String instanceHrid = null;
		if ( id.length() > 30 ) {
			instanceId = id;
			if ( instanceHridById == null )
				instanceHridById = inventory.prepareStatement(
						"SELECT hrid FROM instanceFolio WHERE id = ?");
			instanceHridById.setString(1,instanceId);
			try ( ResultSet rs = instanceHridById.executeQuery() ) {
				while (rs.next()) instanceHrid = rs.getString(1);
			}
			if (instanceHrid == null) {
				System.out.printf("instance %s not in instanceFolio\n", instanceId); return null;
			}
		} else {
			instanceHrid = id;
		}
		if ( bibByInstanceHrid == null )
				bibByInstanceHrid = inventory.prepareStatement(
						"SELECT * FROM bibFolio WHERE instanceHrid = ?");
		bibByInstanceHrid.setString(1, instanceHrid);
		String marc = null;
		try ( ResultSet rs = bibByInstanceHrid.executeQuery() ) {
			while (rs.next()) marc = rs.getString("content").replaceAll("\\s*\\n\\s*", " ");
		}
		if ( marc != null ) return jsonToMarcRec( marc );

		if ( instanceId == null ) {
			if ( instanceIdByHrid == null )
				instanceIdByHrid = inventory.prepareStatement(
						"SELECT id FROM instanceFolio WHERE hrid = ?");
			instanceIdByHrid.setString(1,instanceHrid);
			try ( ResultSet rs = instanceIdByHrid.executeQuery() ) {
				while (rs.next()) instanceId = rs.getString(1);
			}
			if (instanceId == null) {
				System.out.printf("instance %s not in instanceFolio\n", instanceHrid); return null;
			}
		}

		OkapiClient okapi = this.config.getOkapi("Folio");
		marc = okapi.query("/source-storage/records/"+instanceId+"/formatted?idType=INSTANCE");
		Matcher m = modDateP.matcher(marc.replaceAll("\\s*\\n\\s*", " "));
		Timestamp marcTimestamp = (m.matches())
				? Timestamp.from(Instant.parse(m.group(1).replace("+0000","Z"))): null;
		if ( replaceBib == null )
			replaceBib = inventory.prepareStatement(
					"REPLACE INTO bibFolio ( instanceHrid, moddate, content ) VALUES (?,?,?)");
		replaceBib.setString(1, instanceHrid);
		replaceBib.setTimestamp(2, marcTimestamp);
		replaceBib.setString(3, marc);
		replaceBib.executeUpdate();
		return jsonToMarcRec( marc );
	}
	private static PreparedStatement instanceHridById = null;
	private static PreparedStatement bibByInstanceHrid = null;
	private static PreparedStatement instanceIdByHrid = null;
	private static PreparedStatement replaceBib = null;
	static Pattern modDateP = Pattern.compile("^.*\"updatedDate\" *: *\"([^\"]+)\".*$");

	@Override
	public List<MarcRecord> retrieveRecordsByIdRange(RecordType type, Integer from, Integer to)
			throws SQLException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public MarcRecord jsonToMarcRec( String marcResponse ) throws IOException {
		Map<String,Object> parsedResults = mapper.readValue(marcResponse, Map.class);
		Map<String,Object> parsedRecord = (Map<String,Object>) parsedResults.get("parsedRecord");
		Map<String,Object> jsonStructure = (Map<String,Object>)parsedRecord.get("content");
		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.leader = (String) jsonStructure.get("leader");
		List<Map<String,Object>> fields = (List<Map<String, Object>>) jsonStructure.get("fields");
		int fieldId = 1;
		for ( Map<String,Object> f : fields )
			for ( Entry<String,Object> field : f.entrySet() ) {
				Object fieldValue = field.getValue();
				if ( fieldValue.getClass().equals(String.class) ) {
					rec.controlFields.add(new ControlField(fieldId++,field.getKey(),
							Normalizer.normalize((String) fieldValue,Normalizer.Form.NFC)));
					if (field.getKey().equals("001")) {
						rec.bib_id = (String) fieldValue; rec.id = rec.bib_id;
					}
				} else {
					Map<String,Object> fieldContent = (Map<String, Object>) fieldValue;
					int subfieldId = 1;
					List<Map<String,Object>> subfields = (List<Map<String,Object>>) fieldContent.get("subfields");
					TreeSet<Subfield> processedSubfields = new TreeSet<>();
					for (Map<String,Object> subfield : subfields) {
						if ( subfield.isEmpty() ) continue;
						String code = subfield.keySet().iterator().next();
						processedSubfields.add(new Subfield( subfieldId++, code.charAt(0),
								Normalizer.normalize((String) subfield.get(code),Normalizer.Form.NFC) ));
					}
					rec.dataFields.add(new DataField(fieldId++,field.getKey(),
							((String)fieldContent.get("ind1")).charAt(0),
							((String)fieldContent.get("ind2")).charAt(0),
							processedSubfields
							));
				}
			}
		F: for ( DataField f : rec.dataFields )
			for ( Subfield sf : f.subfields )
				if ( sf.code.equals('6') )
					if (subfield6Pattern.matcher(sf.value).matches()) {
						if (f.tag.equals("880"))
							f.mainTag = sf.value.substring(0,3);
						f.linkNumber = Integer.valueOf(sf.value.substring(4,6));
						continue F;
					}

		return rec;
	}
	private static ObjectMapper mapper = new ObjectMapper();

	private static Pattern subfield6Pattern = Pattern.compile("[0-9]{3}-[0-9]{2}.*");

}
