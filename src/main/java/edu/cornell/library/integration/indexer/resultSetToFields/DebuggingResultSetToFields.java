package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Print result set to STDOUT and execute inner ResultSetToFields
 *
 */
public class DebuggingResultSetToFields implements ResultSetToFields {
	
	ResultSetToFields innerRstf;
	
	public DebuggingResultSetToFields(ResultSetToFields innerRstf) {
		super();
		this.innerRstf = innerRstf;
	}

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		System.out.print( makeDebugStr(results) );
		return innerRstf.toFields(results, config);
	}

	String makeDebugStr(Map<String, ResultSet> results) {
		if( results == null )
			return "The Map of ResultSets was Null!";
		String out ="";
		for( String key : results.keySet()){
			out = out + "ResultSet " + key + "\n";
			out = out + resultsToStr( results.get(key));
		}
		return out;
	}

	private String resultsToStr(ResultSet resultSet) {
		if( resultSet == null ){
			return "    null";
		}
		String out ="";
		while( resultSet.hasNext()){
			QuerySolution qs = resultSet.nextSolution();			
			out = out + qsToStr(qs) ;
		}
		return out;
	}

	private String qsToStr(QuerySolution qs) {
		if(qs == null )
			return  "    null QuerySolution\n";
		
		String out = "";
		Iterator<String> it = qs.varNames();
		while(it.hasNext()){
			String varName = it.next();
			out = out 
					+ "    " + varName + ": '" 
					+ ResultSetUtilities.nodeToString( qs.get(varName) ) 
					+ "'";
		}
		return out + "\n";
	}

}
