package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

public class ResultsToStdout implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) {		
		
		for( String resultKey: results.keySet()){
			System.out.println("result set for query '"+resultKey + "'");
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){													
						System.out.println( nodeToString(sol.get(names.next())));
					}
				}
			}
		}
		
		return Collections.emptyMap();
	}	
}
