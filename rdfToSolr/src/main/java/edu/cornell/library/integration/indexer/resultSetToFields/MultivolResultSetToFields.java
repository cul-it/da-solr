	package edu.cornell.library.integration.indexer.resultSetToFields;
	
	import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
	
	
	public class MultivolResultSetToFields implements ResultSetToFields {
	
		String fieldName = "multivol_b";
		boolean found = false;
		
		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) {
			
			ALL: for( String resultKey: results.keySet()){
				ResultSet rs = results.get(resultKey);
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						Iterator<String> names = sol.varNames();
						while(names.hasNext() ){						
							String name = names.next();
							RDFNode node = sol.get(name);
							if( node != null ) {
								System.out.println(nodeToString(node));
								found = true;
								break ALL;
							}
						}
					}
				}
			}
			
			SolrInputField field = new SolrInputField(fieldName);
			field.setValue(found, 1.0f);
			return Collections.singletonMap(fieldName,field);
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
