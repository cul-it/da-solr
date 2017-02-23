package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * Build Call number display and facet fields in two steps. 
 * All code is executed in each pass, so it needs to have necessary conditionals.
 */
public class Title130ResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		Map<String,SolrInputField> solrFields = new HashMap<>();
						
		MarcRecord rec = new MarcRecord();

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			rec.addDataFieldResultSet(rs,"130");
		}
		
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();

		for( FieldSet fs: sortedFields.values() ) {

			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			for (DataField f: fs.fields) {
				String field = f.concatenateSpecificSubfields("adfgklmnoprst");
				String cts = f.concatenateSpecificSubfields("adfgklmnoprst");
				String titleWOarticle = f.getStringWithoutInitialArticle(field);
				if (f.tag.equals("880")) {
					if (f.getScript().equals(MarcRecord.Script.CJK)) {
						addField(solrFields,"title_uniform_t_cjk",field);
					} else {
						if (hasCJK(field))
							addField(solrFields,"title_uniform_t_cjk",field);
						addField(solrFields,"title_uniform_t",field);
						addField(solrFields,"title_uniform_t",titleWOarticle);
					}
				} else {
					addField(solrFields,"title_uniform_t",field);
					addField(solrFields,"title_uniform_t",titleWOarticle);
				}
				if (cts.length() > 0) {
					field += "|"+cts;
				}
				if (f.tag.equals("880")) {
					values880.add(field);
				} else {
					valuesMain.add(field);
				}
			}
			for (String s: values880)
				addField(solrFields,"title_uniform_display",s);	
			for (String s: valuesMain)
				addField(solrFields,"title_uniform_display",s);	
		}
				
		return solrFields;	
	}

}
