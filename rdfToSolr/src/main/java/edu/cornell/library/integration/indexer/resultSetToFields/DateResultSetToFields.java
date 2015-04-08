package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.util.StringUtils;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;

/**
 * processing date result sets into fields pub_date_facet, pub_date_sort, pub_date_display
 * 
 */
public class DateResultSetToFields implements ResultSetToFields {

	static final Pattern p = Pattern.compile("^[0-9]{4}$");
	static final int current_year = Calendar.getInstance().get(Calendar.YEAR);
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();

		//Checking to avoid duplicate sort/facet dates due to a very small number of records
		// with duplicate 008 fields.
		Boolean found_single_date = false;

		//Collecting all of the display dates to provide further deduping, then concatenation.
		Collection<String> pub_date_display = new HashSet<String>(); //hashset drops duplicates

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if( rs == null) continue;
			while(rs.hasNext()){
				QuerySolution sol = rs.nextSolution();


				if (resultKey.equals("machine_dates") && ! found_single_date) {
					String eight = nodeToString(sol.get("eight"));
					if (eight.length() < 15) continue;
					String date = null;
					// using second 008 date value in some cases DISCOVERYACCESS-1438
					switch(eight.charAt(6)) {
					case 'p':
					case 'r':
						date = eight.substring(11, 15);
						break;
					default:
						date = eight.substring(7, 11);
					}
					Matcher m = p.matcher(date);
					if ( ! m.matches()) continue;
					int year = Integer.valueOf(date);
					if (year == 9999) continue;
					if (year > current_year + 1) {
						// suppress future dates from facet and sort
					} else if (year > 0) {
						addField(fields,"pub_date_sort",date);
						addField(fields,"pub_date_facet",date);
						found_single_date = true;
					}


				} else if (resultKey.startsWith("human_dates")) {
					String date = nodeToString(sol.get("date"));
					if (sol.contains("ind2")) {
						String ind2 = nodeToString(sol.get("ind2"));
						if (ind2.equals("0") || ind2.equals("2") || ind2.equals("3"))
							continue;
					}
					pub_date_display.add(removeTrailingPunctuation(date, 
							date.contains("[") ?  ". " : "]. "));
				}
			}
		}
		pub_date_display = dedupe_pub_dates(pub_date_display);
		if (pub_date_display.size() > 0)
			addField(fields,"pub_date_display",StringUtils.join(" ", pub_date_display));
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
