package edu.cornell.library.integration.metadata.generator;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.marc.DataField.RLE_openRTL;

import java.util.Arrays;
import java.util.List;

import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * processing into contents_display and partial_contents_display
 * 
 */
public class TOC implements SolrFieldGenerator {

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("505"); }

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config config ) {

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
