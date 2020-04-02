package edu.cornell.library.integration.metadata.generator;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Process Uniform title in field 130.
 */
public class Title130 implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("130"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {
		SolrFields sfs = new SolrFields();
		for( DataField f: rec.matchSortAndFlattenDataFields() ) {

			String field = f.concatenateSpecificSubfields("adfgklmnoprst");
			String cts = f.concatenateSpecificSubfields("adfgklmnoprst");
			String titleWOarticle = f.getStringWithoutInitialArticle(field);
			if (f.tag.equals("880") && f.getScript().equals(DataField.Script.CJK))
				sfs.add(new SolrField("title_uniform_t_cjk",field));
			else {
				sfs.add(new SolrField("title_uniform_t",field));
				sfs.add(new SolrField("title_uniform_t",titleWOarticle));
			}
			if (cts.length() > 0)
				field += "|"+cts;

			sfs.add(new SolrField("title_uniform_display",field));
		}

		return sfs;	
	}

}
