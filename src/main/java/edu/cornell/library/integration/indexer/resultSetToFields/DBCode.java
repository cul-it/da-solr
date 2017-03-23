package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * 856‡i should be access instructions, but has been used to store code keys
 * that map database records to information about access restrictions
 */
public class DBCode implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();

		Set<String> dbcodes = new HashSet<>();
		Set<String> providercodes = new HashSet<>();
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();

					String instructions = nodeToString(sol.get("v"));
					if (instructions.contains("_")) {
						String[] codes = instructions.split("_",2);
						if (codes.length == 2) {
							providercodes.add(codes[0]);
							dbcodes.add(codes[1]);
						}
					}
				}
			}
		}
		for ( String dbcode : dbcodes )
			addField(fields,"dbcode",dbcode);
		for ( String providercode : providercodes )
			addField(fields,"providercode",providercode);
		return fields;
	}	

}
