package edu.cornell.library.integration.indexer.resultSetToFields;
	
import java.util.Collections;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
	
	
	public class MultivolResultSetToFields implements ResultSetToFields {
	
		protected boolean debug = false;

		String fieldName = "multivol_b";
		boolean multivol = false;
		
		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) {
			
			ALL: for( String resultKey: results.keySet()){
				ResultSet rs = results.get(resultKey);
				if( rs != null){
					while(rs.hasNext()){
						QuerySolution sol = rs.nextSolution();
						String rectype = nodeToString(sol.get("rectype"));
						if (debug) System.out.println( "Holdings record type: "+rectype);
						if (rectype.equals("y")) {
							multivol = true;
							if (debug) System.out.println("multivol true due to rectype y" +rectype);
						} else {
							if (sol.contains("f") &&
									nodeToString(sol.get("f")).contains("http") &&
									! rectype.equals("v")) {
								multivol = true;
								if (debug) System.out.println("multivol true due to presence of 866/867/868 and rectype not v");
							}
						}
						if (multivol)
							break ALL;
					}
				}
			}
			
			SolrInputField field = new SolrInputField(fieldName);
			field.setValue(multivol, 1.0f);
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
