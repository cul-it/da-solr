	package edu.cornell.library.integration.indexer.resultSetToFields;
	
	import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
	
	
	public class NameFieldsAsColumnsRSTF implements ResultSetToFields {
	
		protected boolean debug = false;
		
		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) {

			//This method needs to return a map of fields:
			Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
			
			for( String resultKey: results.keySet()){
				ResultSet rs = results.get(resultKey);
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						Iterator<String> names = sol.varNames();
						while(names.hasNext() ){						
							String name = names.next();
							if (debug) System.out.println(name + ": " + nodeToString(sol.get(name)));
							addField(fields,name,nodeToString(sol.get(name)));
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
