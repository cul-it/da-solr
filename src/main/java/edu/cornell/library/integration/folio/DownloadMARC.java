package edu.cornell.library.integration.folio;

import java.io.IOException;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
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

	@Override
	public MarcRecord getMarc(RecordType type, String id) throws SQLException, IOException, InterruptedException {
		if (! type.equals(RecordType.BIBLIOGRAPHIC) )
			throw new IllegalArgumentException(
					String.format("Folio only contains Bibliographic MARC. Request for (%s, %s) invalid\n",type,id));
		OkapiClient okapi = this.config.getOkapi("Folio");
		// /source-storage/records/a8e22ecf-ee59-4ef8-95a0-40155a4ac100/formatted?idType=INSTANCE
		String instanceId;
		if ( id.length() > 30 ) 
			instanceId = id;
		else {
			List<Map<String,Object>> instances = okapi.queryAsList("/instance-storage/instances", "hrid=="+id);
			if ( instances.isEmpty() ) throw new IOException ("Instance not found (hrid:"+id+").");
			if ( instances.size() > 1 )
				throw new IOException("Multiple instances ("+instances.size()+") found for hrid "+id);
			instanceId = (String) instances.get(0).get("id");
		}
		System.out.println(instanceId);
		String results = okapi.query("/source-storage/records/"+instanceId+"/formatted?idType=INSTANCE");
		Map<String,Object> parsedResults = mapper.readValue(results, Map.class);
		Map<String,Object> parsedRecord = (Map<String,Object>) parsedResults.get("parsedRecord");
		System.out.println (mapper.writeValueAsString(parsedRecord));
		return jsonToMarcRec((Map<String,Object>)parsedRecord.get("content"));
	}

	@Override
	public List<MarcRecord> retrieveRecordsByIdRange(RecordType type, Integer from, Integer to)
			throws SQLException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public MarcRecord jsonToMarcRec( Map<String,Object> jsonStructure ) {
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
