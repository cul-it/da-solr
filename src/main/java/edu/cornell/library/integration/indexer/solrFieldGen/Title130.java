package edu.cornell.library.integration.indexer.solrFieldGen;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Process Uniform title in field 130.
 */
public class Title130 implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		rec.addDataFieldResultSet(results.get("title_130"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	public static SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {
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