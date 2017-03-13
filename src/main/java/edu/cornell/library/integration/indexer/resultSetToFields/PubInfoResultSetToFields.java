package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class PubInfoResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> solrFields = new HashMap<>();
		for( FieldSet fs: sets ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			String relation;
			String publisher = null;
			String pubplace = null;
			String publisherVern = null;
			String pubplaceVern = null;
			switch (fs.fields.iterator().next().ind2) {
			case '0': relation = "pub_prod"; break;
			case '1': relation = "pub_info"; break;
			case '2': relation = "pub_dist"; break;
			case '3': relation = "pub_manu"; break;
			case '4': relation = "pub_copy"; break;
			default: relation = "pub_info";
			}
			for (DataField f: fs.fields) {
				if (f.tag.equals("880")) {
					values880.add(f.concatenateSubfieldsOtherThan("6"));
					if (relation.equals("pub_info")) {
						publisherVern = f.concatenateSpecificSubfields("b");
						pubplaceVern = f.concatenateSpecificSubfields("a");
					}
				} else {
					valuesMain.add(f.concatenateSubfieldsOtherThan("6"));
					if (relation.equals("pub_info")) {
						publisher = f.concatenateSpecificSubfields("b");
						pubplace = f.concatenateSpecificSubfields("a");
					}
				}
			}
			for (String s: values880)
				addField(solrFields,relation+"_display",s);	
			for (String s: valuesMain)
				addField(solrFields,relation+"_display",s);
			if (pubplace != null) {
				if (pubplaceVern != null)
					addField(solrFields,"pubplace_display",pubplaceVern +" / "+ pubplace);
				else
					addField(solrFields,"pubplace_display",pubplace);
			} else if (pubplaceVern != null)
				addField(solrFields,"pubplace_display",pubplaceVern);

			if (publisher != null) {
				if (publisherVern != null)
					addField(solrFields,"publisher_display",publisherVern +" / "+ publisher);
				else
					addField(solrFields,"publisher_display",publisher);
			} else if (publisherVern != null)
				addField(solrFields,"publisher_display",publisherVern);
		}
				
		return solrFields;
	}	
}
