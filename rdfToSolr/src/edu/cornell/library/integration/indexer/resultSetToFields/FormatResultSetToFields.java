package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
public class FormatResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		String category;
		String record_type;
		String bibliographic_level;
						
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					if (resultKey.equals("format")) {
						
					}
				}
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
