package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.RLE_openRTL;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing into contents_display and partial_contents_display
 * 
 */
public class TOCResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> solrFields = new HashMap<>();
		for( FieldSet fs: sets ) {

			List<String> values880 = new ArrayList<>();
			List<Boolean> isCJK = new ArrayList<>();
			List<String> valuesMain = new ArrayList<>();
			String relation = null;
			String subfields = "atr";
			for (DataField f: fs.fields) {
				if(relation == null) {
					if (f.ind1.equals('1') || f.ind1.equals('8')) {
						relation = "contents";
					} else if (f.ind1.equals('0')) {
						relation = "contents";
						subfields = "agtr";
					} else if (f.ind1.equals('2')) {
						relation = "partial_contents";
					}
				}
				if (f.tag.equals("880")) {
					values880.add(f.concatenateSpecificSubfields(subfields));
					if (f.getScript().equals(MarcRecord.Script.CJK)) 
						isCJK.add(true);
					else
						isCJK.add(false);
				} else
					valuesMain.add(f.concatenateSpecificSubfields(subfields));
			}
			if (relation != null) {
				Boolean exactlyOne880 = (values880.size() == 1);
				Iterator<String> i = values880.iterator();
				Iterator<Boolean> cjk_i = isCJK.iterator();
				while (i.hasNext()) {
					String s = i.next();
					Boolean cjk = cjk_i.next();
					if (exactlyOne880) {
						for(String item: s.split(" *-- *")) {
							if (s.endsWith(PDF_closeRTL)) {
								if (! item.startsWith(RLE_openRTL))
									item = RLE_openRTL + item;
								if (! item.endsWith(PDF_closeRTL))
									item += PDF_closeRTL;
							}
							addField(solrFields,relation+"_display",item);
							if (cjk)
								addField(solrFields,"toc_t_cjk",item);
							else {
								if (hasCJK(item))
									addField(solrFields,"toc_t_cjk",item);
								addField(solrFields,"toc_t",item);
							}
						}
					} else {
						addField(solrFields,relation+"_display",s);
						if (cjk)
							addField(solrFields,"toc_t_cjk",s);
						else {
							if (hasCJK(s))
								addField(solrFields,"toc_t_cjk",s);
							addField(solrFields,"toc_t",s);
						}
					}
				}
				for (String s: valuesMain)
					if (valuesMain.size() == 1) {
						for (String item: s.split(" *-- *")) {
							addField(solrFields,relation+"_display",item);
							addField(solrFields,"toc_t",item);
							if (isCJK(item))
								addField(solrFields,"toc_t_cjk",item);
						}
					} else {
						addField(solrFields,relation+"_display",s);
						addField(solrFields,"toc_t",s);
						if (isCJK(s))
							addField(solrFields,"toc_t_cjk",s);
					}
			}
		}
		
		return solrFields;
	}	

}
