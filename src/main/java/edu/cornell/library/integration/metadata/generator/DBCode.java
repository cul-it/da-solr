package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * 856‡i should be access instructions, but has been used to store code keys
 * that map database records to information about access restrictions
 */
public class DBCode implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("899"); }

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {
		SolrFields vals = new SolrFields();
		for (DataField f : rec.dataFields)
			for (Subfield sf : f.subfields) if (sf.code.equals('a'))
				if (sf.value.contains("_")) {
					String[] codes = sf.value.split("_",2);
					if (codes.length == 2) {
						vals.add(new SolrField("providercode",codes[0]));
						vals.add(new SolrField("dbcode",codes[1]));
					}
				}
		return vals;
	}
}
