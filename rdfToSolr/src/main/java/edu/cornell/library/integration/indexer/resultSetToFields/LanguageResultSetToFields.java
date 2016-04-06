package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class LanguageResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
		Collection<String> facet_langs = new HashSet<String>();
		List<String> display_langs = new ArrayList<String>();

		// Suppress "Undetermined" and "No Linguistic Content"
		// from facet and display (DISCOVERYACCESS-822)
		
		ResultSet rs = results.get("language_main");
		if (rs != null && rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			String language = nodeToString(sol.get("language"));
			if ( ! language.equalsIgnoreCase("Undetermined")
					&& ! language.equalsIgnoreCase("No Linguistic Content") ) {
				display_langs.add(language);
				facet_langs.add(language);		
			}
		}

		rs = results.get("languages_041");
		if (rs != null)
			while (rs.hasNext()) {
				QuerySolution sol = rs.nextSolution();
				String language = nodeToString(sol.get("language"));
				if ( ! language.equalsIgnoreCase("Undetermined")
						&& ! language.equalsIgnoreCase("No Linguistic Content")
						&& ! display_langs.contains(language) ) {
					String subfield_code = nodeToString(sol.get("c"));
					display_langs.add(language);
					if ("adegj".contains(subfield_code)) {
						facet_langs.add(language);
					}
				}
			}

		// language note
		MarcRecord rec = new MarcRecord();
		rec.addDataFieldResultSet(results.get("language_note"),"546");
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
		for (FieldSet fs : sortedFields.values()) {
			String value880 = null;
			String valueMain = null;
			for (DataField f: fs.fields) {
				if (f.tag.equals("880")) {
					value880 = f.concatenateSpecificSubfields("3ab");
				} else {
					valueMain = f.concatenateSpecificSubfields("3ab");
				}
			}
			if (valueMain == null && value880 != null) {
				display_langs.add(value880);
			} else if (valueMain != null) {
				for (String language : display_langs)
					if (valueMain.contains(language))
						display_langs.remove(language);
				if (value880 != null) {
					if (value880.length() <= 15) {
						display_langs.add(value880+" / " + valueMain);
					} else {
						display_langs.add(value880);
						display_langs.add(valueMain);
					}
				} else {
					display_langs.add(valueMain);
				}
			}
		}


		Iterator<String> i = facet_langs.iterator();
		while (i.hasNext()) addField(fields,"language_facet",i.next());
		StringBuilder sb = new StringBuilder();
		i = display_langs.iterator();
		Boolean first = true;
		while (i.hasNext()) {
			if (first) first = false;
			else sb.append(", ");
			sb.append(i.next());
		}
		addField(fields, "language_display",sb.toString());
		
		return fields;
	}


}
