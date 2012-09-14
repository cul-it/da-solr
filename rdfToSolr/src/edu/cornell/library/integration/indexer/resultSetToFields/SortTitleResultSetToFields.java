package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class SortTitleResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();		
						
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);						
			while(rs != null && rs.hasNext()){
				QuerySolution sol = rs.nextSolution();					
				String title =nodeToString(sol.get("title"));
					
				//offset is used to strip of any articles from title						
				Integer offset = Integer.valueOf(nodeToString(sol.get("ind2")));						
				addField(fields,"title_sort",title.substring(offset).toLowerCase());										
			} 
		}									
		return fields;
	}


}
