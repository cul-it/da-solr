package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

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
					sfs.add(new SolrField("id_t_cjk",sf.value));
				else
					sfs.add(new SolrField("id_t",sf.value));

				// display fields
				switch (f.tag) {
				case "024":
				case "035":
					if (sf.value.startsWith("(OCoLC)"))
						sfs.add(new SolrField("oclc_id_display",sf.value.substring(7).trim()));
					else if ( knownVocab != null )
						sfs.add(new SolrField(knownVocab+"_display",sf.value));
					else
						sfs.add(new SolrField("other_id_display",sf.value));
					break;
				case "028":
					sfs.add(new SolrField("publisher_number_display",sf.value));
					break;
				}
			}
	}
		return sfs;
	}
}
