package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * Using control fixed fields present in Book type MARC bib records to identify
 * Fact and Fiction records. Some control field values, such as 'poetry' are
 * ambiguous.
 */
public class FactOrFiction implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();
		String chars6and7 = "";
		String char33 = "";

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if (name.equals("char33")) 
							char33 = nodeToString( node );
						else
							chars6and7 = nodeToString( node );
					}
				}
			}
		}

		if (chars6and7.equalsIgnoreCase("aa") 
			|| chars6and7.equalsIgnoreCase("ac")
			|| chars6and7.equalsIgnoreCase("ad")
			|| chars6and7.equalsIgnoreCase("am")
			|| chars6and7.startsWith("t")
			) {
			if (char33.equals("0") ||
					char33.equalsIgnoreCase("i")) {
				addField(fields,"subject_content_facet","Non-Fiction (books)");
			} else if (char33.equals("1") ||
					char33.equalsIgnoreCase("d") ||
					char33.equalsIgnoreCase("f") ||
					char33.equalsIgnoreCase("j")) {
				addField(fields,"subject_content_facet","Fiction (books)");
			}
		}
		
		return fields;

	}
}
