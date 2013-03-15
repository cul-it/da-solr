package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class FactOrFictionResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		String char6 = "";
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
							char6 = nodeToString( node );
					}
				}
			}
		}

		if (char6.equalsIgnoreCase("a")) {
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


	private String nodeToString( RDFNode node){
		if( node == null )
			return "";
		else if ( node.canAs( Literal.class )){
			return ((Literal)node).getLexicalForm();			
		}else {
			return node.toString();
		}
	}

}
