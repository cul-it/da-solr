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
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class DateResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();		
				
		Pattern p = Pattern.compile("^[0-9]{4}$");
		
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs != null){
				while(rs.hasNext()){
					QuerySolution sol = rs.nextSolution();
					Iterator<String> names = sol.varNames();
					while(names.hasNext() ){						
						String name = names.next();
						RDFNode node = sol.get(name);
						if( node != null ) {
							if (resultKey.equals("human_dates")) {
								fields.put("pub_date_display", 
										new SolrInputField(nodeToString(node)));
							} else if (resultKey.equals("machine_dates")) {
								Matcher m = p.matcher(nodeToString(node));
								if (m.matches()) {
										if (name.equals("date1")) {
											fields.put("date_pub_sort",
													new SolrInputField(nodeToString(node)));
											if (fields.containsKey("date_pub")) {
											fields.get("date_pub").addValue(nodeToString(node), 1);
										} else {
											fields.put("date_pub",
													new SolrInputField(nodeToString(node)));										
										}
									} else {
										if (3000 > Integer.parseInt(nodeToString(node))) {
											if (fields.containsKey("date_pub")) {
												fields.get("date_pub").addValue(nodeToString(node), 1);
											} else {
												fields.put("date_pub",
														new SolrInputField(nodeToString(node)));										
											}
										}
									}
								} else {
									// debug stmt should be added. Node is not a date in \d\d\d\d format.
								}
							}
						}
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
