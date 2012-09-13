package edu.cornell.library.integration.indexer.resultSetToFields;

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
public class FormatResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		String category ="";
		String record_type ="";
		String bibliographic_level ="";
		String format = null;
						
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if (name.equals("cat")) {
							category = nodeToString( node );
						} else if (name.equals("rectype")) {
							record_type = nodeToString( node );
						} else if (name.equals("biblvl")) {
							bibliographic_level = nodeToString( node );
						}
					}
				}
			}
		}
		
		if (record_type.equals("a")) {
			if ((bibliographic_level.equals("a"))
					|| (bibliographic_level.equals("m"))) {
				format = "Book";
			} else if ((bibliographic_level.equals("b"))
					|| (bibliographic_level.equals("s"))) {
				format = "Serial";
			}
		} else if (record_type.equals("t")) {
			if ((bibliographic_level.equals("a"))
					|| (bibliographic_level.equals("m"))) {
				format = "Book";
			}
		} else if ((record_type.equals("c"))
				|| (record_type.equals("d"))) {
			format = "Musical Score";
		} else if ((record_type.equals("e"))
				|| (record_type.equals("f"))) {
			format = "Map or Globe";
		} else if (record_type.equals("i")) {
			format = "Non-musical Recording";
		} else if (record_type.equals("j")) {
			format = "Musical Recording";
		} else if (record_type.equals("k")) {
			format = "Image";
		} else if (record_type.equals("m")) {
			format = "Computer File";
		} else if (category.equals("h")) {
			format = "Microform";
		} else if (category.equals("q")) {
			format = "Musical Score";
		} else if (category.equals("v")) {
			format = "Video";
		} else {
			format = "Unknown";
		}
		
		SolrInputField format_field = new SolrInputField("format");
		format_field.addValue(format, 1);
		fields.put("format", format_field);
		
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
