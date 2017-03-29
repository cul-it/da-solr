package edu.cornell.library.integration.indexer.resultSetToFields;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;

/**
 * Process 856 fields from both bibliographic and holdings fields into various URL Solr fields.
 * 
 */
public class URL implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		Collection<FieldSet> sets = ResultSetUtilities.resultSetsToSetsofMarcFields(results);

		Map<String,SolrInputField> fields = new HashMap<>();
		for( FieldSet fs: sets ) {

			SolrFieldValueSet vals = generateSolrFields( fs );
			for ( SolrField f : vals.fields )
				ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue, true);

		}
		return fields;
	}

	public static SolrFieldValueSet generateSolrFields ( FieldSet fs ) throws IOException  {
		Set<String> linkTexts = new HashSet<>();
		Set<String> urls = new HashSet<>();
		Map<String,Object> jsonModel = new HashMap<>();

		String instructions = null;
		for (DataField f : fs.fields) {
			urls.addAll(f.valueListForSpecificSubfields("u"));
			instructions = f.concatenateSpecificSubfields("i");
			String linkLabel = f.concatenateSpecificSubfields("3yz");
			if (linkLabel.isEmpty())
				continue;
			linkTexts.add(linkLabel);
		}
		if (instructions != null &&
				(instructions.contains("dbcode") || instructions.contains("providercode"))) {
			String[] codes = instructions.split(";\\s*");
			for (String code : codes) {
				String[] parts = code.split("=",2);
				if (parts.length == 2)
					if (parts[0].equals("dbcode")) {
						jsonModel.put("dbcode",parts[1]);
					} else if (parts[0].equals("providercode")) {
						jsonModel.put("providercode",parts[1]);
					}
			}
		}

		if ( ! linkTexts.isEmpty())
			jsonModel.put( "description", String.join(" ",linkTexts) );
		String relation ="access"; //this is a default and may change later
		if (jsonModel.containsKey("description")) {
			String lc = ((String)jsonModel.get("description")).toLowerCase();
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
					|| lc.contains("more information")
					|| lc.equals("hathitrust – access limited to full-text search")) {
				relation = "other";
			}	
			if (lc.contains("finding aid"))
				relation = "findingaid";
		}

		SolrFieldValueSet vals = new SolrFieldValueSet();
		// There shouldn't be multiple URLs, but we're just going to iterate through
		// them anyway.
		for (String url: urls) {
			String urlRelation = relation;
			String url_lc = url.toLowerCase();
			if (url_lc.contains("://plates.library.cornell.edu")) {
				urlRelation = "bookplate";
				if (jsonModel.containsKey("description"))
					vals.fields.add( new SolrField ("donor_t", ((String)jsonModel.get("description")) ));
				vals.fields.add( new SolrField ("donor_s", url.substring(url.lastIndexOf('/')+1)) );
			} else if (url.toLowerCase().contains("://pda.library.cornell.edu")) {
				urlRelation = "pda";
			}
			if ( ! jsonModel.containsKey("description")) {
				vals.fields.add( new SolrField ("url_"+urlRelation+"_display",url));						
			} else {
				vals.fields.add( new SolrField ("url_"+urlRelation+"_display",url + "|" + jsonModel.get("description")));
				vals.fields.add( new SolrField ("notes_t",((String)jsonModel.get("description"))));
			}
			if (urlRelation.equals("access")) {
				jsonModel.put("url", url);
				ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, jsonModel);
				vals.fields.add( new SolrField("url_access_json",jsonstream.toString("UTF-8")) );
			}
		}

		return vals;
	}	

	public static class SolrFieldValueSet {
		List<SolrField> fields = new ArrayList<>();
	}
}
