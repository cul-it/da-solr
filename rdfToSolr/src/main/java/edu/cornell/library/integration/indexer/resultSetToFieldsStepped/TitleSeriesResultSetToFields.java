package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class TitleSeriesResultSetToFields implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results) throws Exception {
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();
		
		MarcRecord rec = new MarcRecord();
		String tag = "";
		
		for( String resultKey: results.keySet()){
			tag = resultKey.substring(resultKey.length() - 3);
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
			Set<String> values880 = new HashSet<String>();
			Set<String> valuesMain = new HashSet<String>();
			String cts880 = null, ctsMain = null;
			for (DataField f: dataFields)
				if (f.tag.equals("880")) {
					values880.add(f.concatenateSubfieldsOtherThanSpecified("6"));
					cts880 = f.concatenateSpecificSubfields("abcdefghijklmnopqrstuwxyz");//no "v"
				} else {
					valuesMain.add(f.concatenateSubfieldsOtherThanSpecified("6"));
					ctsMain = f.concatenateSpecificSubfields("abcdefghijklmnopqrstuwxyz");//no "v"
				}
			if ((values880.size() > 0) || (valuesMain.size() > 0)) {
				for (String s: values880) {
					addField(solrFields,"title_series_display",s);
					addField(solrFields,"title_series_cts",s+"|"+cts880);
				}
				for (String s: valuesMain) {
					addField(solrFields,"title_series_display",s);
					addField(solrFields,"title_series_cts",s+"|"+ctsMain);
				}
				step.setFields(solrFields);
				return step;
			}
		}
			
		String nexttag = "";
		if      (tag.equals("830")) { nexttag = "490"; } 
		else if (tag.equals("490")) { nexttag = "440"; }
		else if (tag.equals("440")) { nexttag = "400"; }
		else if (tag.equals("400")) { nexttag = "410"; }
		else if (tag.equals("410")) { nexttag = "411"; }
		else if (tag.equals("411")) { nexttag = "800"; }
		else if (tag.equals("800")) { nexttag = "810"; }
		else if (tag.equals("810")) { nexttag = "811"; } 

		if (nexttag.equals("")) {
			return step;
		} else {
			step.addMainStoreQuery("title_series_"+nexttag, 
					"SELECT *\n" +
				    " WHERE {\n" +
			        "  $recordURI$ marcrdf:hasField"+nexttag+" ?field.\n" +
				    "  ?field marcrdf:tag ?tag.\n" +
				    "  ?field marcrdf:ind2 ?ind2.\n" +
				   	"  ?field marcrdf:ind1 ?ind1.\n" +
					"  ?field marcrdf:hasSubfield ?sfield.\n" +
					"  ?sfield marcrdf:code ?code.\n" +
					"  ?sfield marcrdf:value ?value. }");
			return step;
		}
	}
}
