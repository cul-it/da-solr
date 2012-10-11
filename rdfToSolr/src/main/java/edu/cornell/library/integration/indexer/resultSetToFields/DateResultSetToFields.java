package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
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
							String value = nodeToString(node);
							if (resultKey.equals("human_dates")) {
								addField(fields, "pub_date_display", RemoveTrailingPunctuation(value,"."));
							} else if (resultKey.equals("machine_dates")) {
								Matcher m = p.matcher(value);
								if (m.matches()) {
									addField(fields,"pub_date",value);
									if (name.equals("date1")) {
										addField(fields,"pub_date_sort",value);
									}									
								} else {
									try{
										if (9999 != Integer.parseInt( value )) 
											addField(fields,"pub_date",value);																			
									}catch(NumberFormatException nfe){
										// not a problem?
									}
								}							
								// debug stmt should be added. Node is not a date in \d\d\d\d format.
							}
						}
					}
				}
			}
		}	
		return fields;
	}	

}
