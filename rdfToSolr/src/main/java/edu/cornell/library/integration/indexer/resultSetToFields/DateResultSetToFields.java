package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class DateResultSetToFields implements ResultSetToFields {

	static final Pattern p = Pattern.compile("^[0-9]{4}$");
	static final int current_year = Calendar.getInstance().get(Calendar.YEAR);
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();

		Boolean found_single_date = false;
		Collection<String> pub_date_display = new HashSet<String>(); //hashset drops duplicates

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs == null) continue;
			while(rs.hasNext()){
				QuerySolution sol = rs.nextSolution();
				Iterator<String> names = sol.varNames();
				while(names.hasNext() ){
					RDFNode node = sol.get(names.next());
					if( node == null ) continue;
					String value = nodeToString(node);
					if (resultKey.equals("human_dates")) {
						pub_date_display.add(removeTrailingPunctuation(value, 
								value.contains("[") ?  ". " : "]. "));
					} else if (resultKey.equals("machine_dates")) {
						if (value.length() < 15) continue;
						String date = null;
						// using second 008 date value in some cases DISCOVERYACCESS-1438
						switch(value.charAt(6)) {
						case 'p':
						case 'r':
							date = value.substring(11, 15);
							break;
						default:
							date = value.substring(7, 11);
						}
						Matcher m = p.matcher(date);
						if ( ! m.matches()) continue;
						int year = Integer.valueOf(date);
						if (year == 9999) continue;
						if (year > current_year + 1) {
							// suppress future dates from facet and sort
						} else if ((year > 0) && ! found_single_date) {
							addField(fields,"pub_date_sort",value);
							addField(fields,"pub_date_facet",value);
							found_single_date = true;
						}
					}
				}
			}
		}
		pub_date_display = dedupe_pub_dates(pub_date_display);
		return fields;
	}

	/* If there are multiple dates on a work, and it can be reliably determined that they
	 * all represent the same year, then we'll display just the year instead of the duplicates.
	 * DISCOVERYACCESS-1539
	 */
	private Collection<String> dedupe_pub_dates(Collection<String> l) {
		if (l.size() < 2) return l;
		Collection<String> years = new HashSet<String>(); //hashset drops duplicates
		for (String date : l) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0 ; i < date.length() ; i++) {
				char c = date.charAt(i);
				if (Character.isDigit(c))
					sb.append(c);
			}
			String year = sb.toString();
			if (year.length() == 4)
				years.add(year);
			else
				return l;
		}
		if (years.size() == 1) return years;

		return l;
	}	

}
