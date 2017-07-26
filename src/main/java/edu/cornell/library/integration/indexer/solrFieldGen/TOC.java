package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.marc.DataField.RLE_openRTL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * processing into contents_display and partial_contents_display
 * 
 */
public class TOC implements ResultSetToFields, SolrFieldGenerator {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord rec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		JenaResultsToMarcRecord.addDataFieldResultSet(rec,results.get("table of contents"));

		Map<String,SolrInputField> fields = new HashMap<>();
		SolrFields vals = generateSolrFields( rec, null );

		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);

		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("505"); }

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, SolrBuildConfig config ) {

		SolrFields solrFields = new SolrFields();

		for (DataField f: rec.matchSortAndFlattenDataFields()) {

			String relation = (f.ind1.equals('2')) ? "partial_contents_display" : "contents_display";
			String value = f.concatenateSpecificSubfields("agtr");

			// Populate display value(s)
			solrFields.addAll(splitToc( relation,  value ));

			// Populate search values
			boolean cjk = (f.tag.equals("880") && f.getScript().equals(DataField.Script.CJK));
			String titleField  = (cjk) ?  "title_addl_t_cjk" :  "title_addl_t";
			String authorField = (cjk) ? "author_addl_t_cjk" : "author_addl_t";
			String tocField    = (cjk) ?         "toc_t_cjk" :         "toc_t";
			for ( Subfield sf : f.subfields )
				switch (sf.code) {
				case 'r':
					solrFields.add(new SolrField( authorField, sf.value )); break;
				case 't':
					solrFields.add(new SolrField( titleField, sf.value ));
				}
			solrFields.add(new SolrField(tocField,value));
		}
		return solrFields;
	}

	private static SolrFields splitToc(String relation, String value) {
		SolrFields sfs = new SolrFields();
		boolean rightToLeft = false;
		if (value.endsWith(PDF_closeRTL)) {
			rightToLeft = true;
			value = value.substring(RLE_openRTL.length(), value.length() - PDF_closeRTL.length());
		}
		for(String item: value.split(" *-- *")) {
			if (rightToLeft)
				item = RLE_openRTL + item + PDF_closeRTL;
			sfs.add(new SolrField(relation,item));
		}
		return sfs;
	}
}
