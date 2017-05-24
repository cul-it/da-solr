package edu.cornell.library.integration.indexer.resultSetToFieldsStepped;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class TitleSeries implements ResultSetToFieldsStepped {

	@Override
	public FieldMakerStep toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		FieldMakerStep step = new FieldMakerStep();
		Map<String,SolrInputField> solrFields = new HashMap<>();
		
		MarcRecord rec = new MarcRecord();
		String tag = "";
		
		for( String resultKey: results.keySet()){
			tag = resultKey.substring(resultKey.length() - 3);
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		Collection<DataFieldSet> sortedFields = rec.matchAndSortDataFields();

		for( DataFieldSet fs: sortedFields ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			String cts880 = null, ctsMain = null;
			for (DataField f: fs.getFields())
				if (f.tag.equals("880")) {
					values880.add(f.concatenateSubfieldsOtherThan("6"));
					cts880 = f.concatenateSpecificSubfields("abcdefghijklmnopqrstuwxyz");//no "v"
				} else {
					valuesMain.add(f.concatenateSubfieldsOtherThan("6"));
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
		}
		step.addMainStoreQuery("title_series_"+nexttag, 
				"SELECT * WHERE {\n"
				+ " BIND( \""+nexttag+"\"^^xsd:string as ?p ) \n"
				+ " $recordURI$ marcrdf:hasField" + nexttag + " ?field.\n"
				+ " ?field marcrdf:tag ?tag. \n"
				+ " ?field marcrdf:ind1 ?ind1. \n"
				+ " ?field marcrdf:ind2 ?ind2. \n"
				+ " ?field marcrdf:hasSubfield ?sfield .\n"
				+ " ?sfield marcrdf:code ?code.\n"
				+ " ?sfield marcrdf:value ?value. }");
		return step;
	}
}
