package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;

public class OtherIDs implements SolrFieldGenerator {
	@Override
	public String getVersion() { return "1.1"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("024","028","035"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {
		SolrFields sfs = new SolrFields();
		for (DataField f : rec.dataFields) {
			String knownVocab = null;
			for (Subfield sf : f.subfields)
				if ( sf.code.equals('2') )
					switch ( sf.value.trim() ) {
					case "doi": knownVocab = "doi"; break;
					case "discogs": knownVocab = "discogs"; break;
					}

			for (Subfield sf : f.subfields) if (sf.code.equals('a')) {

				// search fields
				if (f.getScript().equals(DataField.Script.CJK))
					sfs.add("id_t_cjk",sf.value);
				else
					sfs.add("id_t",sf.value);

				// display fields
				switch (f.tag) {
				case "024":
				case "035":
					if (sf.value.startsWith("(OCoLC)"))
						sfs.add("oclc_id_display",sf.value.substring(7).trim());
					else if ( knownVocab != null )
						sfs.add(knownVocab+"_display",sf.value);
					else
						sfs.add("other_id_display",sf.value);
					break;
				case "028":
					sfs.add("publisher_number_display",sf.value);
					break;
				}
			}
	}
		return sfs;
	}


	public SolrFields generateNonMarcSolrFields(Map<String,Object> instance, Config unused ) {
		SolrFields sfs = new SolrFields();
		if ( ! instance.containsKey("identifiers")) return sfs;
		for (Map<String,String> identifier : (List<Map<String, String>>) instance.get("identifiers")) {

			if (! identifier.containsKey("identifierTypeId")) continue;
			String type = SupportReferenceData.identifierTypes.getName((String)identifier.get("identifierTypeId"));
			String idValue = (String)identifier.getOrDefault("value", null);
			if (idValue == null || idValue.isBlank()) continue;

			switch (type) {
			case "OCLC":
				sfs.add("id_t", idValue);
				if (idValue.startsWith("(OCoLC)"))
					sfs.add("oclc_id_display", idValue.substring(7).trim());
				else
					sfs.add("oclc_id_display", idValue);
				break;
			case "Publisher or distributor number":
				sfs.add("id_t", idValue);
				sfs.add("publisher_number_display", idValue);
				break;
			}
		}

		return sfs;
	}



}
