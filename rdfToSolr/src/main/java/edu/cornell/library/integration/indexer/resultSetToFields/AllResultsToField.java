	package edu.cornell.library.integration.indexer.resultSetToFields;
	
	import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
	
	
	public class AllResultsToField implements ResultSetToFields {
	
		private String fieldName;
	
		public AllResultsToField(String fieldName){
			this.fieldName = fieldName;
		}
		
		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) {

			//This method needs to return a map of fields:
			Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
			
			Set<String> s = new HashSet<String>();
			
			for( String resultKey: results.keySet()){
				ResultSet rs = results.get(resultKey);
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						Iterator<String> names = sol.varNames();
						while(names.hasNext() ){						
							String name = names.next();
							RDFNode node = sol.get(name);
							if( node != null )
								s.add(nodeToString(node));
						}
					}
				}
			}
			
			if (s.size() > 0) {
				Iterator<String> iter = s.iterator();
				while (iter.hasNext()) {
					addField(fields,fieldName,iter.next());
				}
			
				return fields;
			} else {
				return null;
			}
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
