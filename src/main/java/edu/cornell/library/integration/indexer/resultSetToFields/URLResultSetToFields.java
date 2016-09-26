package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class URLResultSetToFields implements ResultSetToFields {

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<>();
		MarcRecord rec = new MarcRecord();
		
		for( String resultKey: results.keySet()){
			rec.addDataFieldResultSet(results.get(resultKey));
		}
		Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
						
		// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
		// but with organization determined by vernMode.
		Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
		Arrays.sort( ids );
		for( Integer id: ids) {
			FieldSet fs = sortedFields.get(id);
			Set<String> values880 = new HashSet<>();
			Set<String> valuesMain = new HashSet<>();
			Set<String> urls = new HashSet<>();
			for (DataField f : fs.fields) {
				urls.addAll(f.valueListForSpecificSubfields("u"));
				String linkLabel = f.concatenateSpecificSubfields("3yz");
				if (linkLabel.isEmpty())
					continue;
				if (f.tag.equals("880"))
					values880.add(linkLabel);
				else
					valuesMain.add(linkLabel);
			}
			values880.addAll(valuesMain);
			String linkDescription = StringUtils.join(values880,' ');
			String relation ="access"; //this is a default and may change later
			String lc = linkDescription.toLowerCase();
			if (lc.contains("table of contents")
					|| lc.contains("tables of contents")
					|| lc.endsWith(" toc")
					|| lc.contains(" toc ")
					|| lc.startsWith("toc ")
					|| lc.equals("toc")
					|| lc.contains("cover image")
					|| lc.equals("cover")
					|| lc.contains("publisher description")
					|| lc.contains("contributor biographical information")
					|| lc.contains("inhaltsverzeichnis")  //table of contents
					|| lc.contains("beschreibung") // description
					|| lc.contains("klappentext") // blurb
					|| lc.contains("buchcover")
					|| lc.contains("publisher's summary")
					|| lc.contains("executive summary")
					|| lc.startsWith("summary")
					|| lc.startsWith("about the")
					|| lc.contains("additional information")
					|| lc.contains("'s website") // eg author's website, publisher's website
					|| lc.startsWith("companion") // e.g. companion website
					|| lc.contains("record available for display")
					|| lc.startsWith("related") // related web site, related electronic resource...
					|| lc.contains("internet movie database")
					|| lc.contains("more information")) {
				relation = "other";
			}
			if (lc.contains("finding aid"))
				relation = "findingaid";
			
			if (urls.size() > 1)
				 System.out.println("Multiple URL fields for an 856 is an error.");
			// There shouldn't be multiple URLs, but we're just going to iterate through
			// them anyway.
			for (String url: urls) {
				String urlRelation = relation;
				String url_lc = url.toLowerCase();
				if (url_lc.contains("://plates.library.cornell.edu")) {
					urlRelation = "bookplate";
					if (! linkDescription.isEmpty())
						addField(fields,"donor_t",standardizeApostrophes(linkDescription));
				} else if (url.toLowerCase().contains("://pda.library.cornell.edu")) {
					urlRelation = "pda";
				}
				if (linkDescription.isEmpty()) {
					addField(fields,"url_"+urlRelation+"_display",url);						
				} else {
					addField(fields,"url_"+urlRelation+"_display",url + "|" + linkDescription);
				}
			}
			if (! urls.isEmpty())
				addField(fields,"notes_t",linkDescription);
		}

		return fields;
	}	

}
