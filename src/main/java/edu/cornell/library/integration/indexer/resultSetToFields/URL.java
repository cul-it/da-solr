package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;

/**
 * Process 856 fields from both bibliographic and holdings fields into various URL Solr fields.
 * 
 */
public class URL implements ResultSetToFields {

	static ObjectMapper mapper = new ObjectMapper();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		Map<String,MarcRecord> holdingRecs = new HashMap<>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ) {
				QuerySolution sol = rs.nextSolution();

				if ( resultKey.equals("urls")) {
					bibRec.addDataFieldQuerySolution(sol);
				} else {
					String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (holdingRecs.containsKey(recordURI)) {
						rec = holdingRecs.get(recordURI);
					} else {
						rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
						holdingRecs.put(recordURI, rec);
					}
					rec.addDataFieldQuerySolution(sol);
				}
			}
		}
		bibRec.holdings.addAll(holdingRecs.values());
		SolrFields vals = generateSolrFields( bibRec, config );
		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		return fields;
	}

	/**
	 * @param config Is unused, but included to follow a consistent method signature. 
	 */
	public static SolrFields generateSolrFields( MarcRecord bibRec, SolrBuildConfig config )
			throws IOException {
		Set<String> linkTexts = new HashSet<>();
		Set<String> urls = new HashSet<>();
		Map<String,Object> jsonModel = new HashMap<>();

		List<DataField> allFields = bibRec.matchSortAndFlattenDataFields();
		for (MarcRecord holdingsRec : bibRec.holdings)
			allFields.addAll(holdingsRec.matchSortAndFlattenDataFields());

		String instructions = null;
		for (DataField f : allFields) {
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
					jsonModel.put(parts[0].toLowerCase(), parts[1]);
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

		SolrFields sfs = new SolrFields();
		// There shouldn't be multiple URLs, but we're just going to iterate through
		// them anyway.
		for (String url: urls) {
			String urlRelation = relation;
			String url_lc = url.toLowerCase();
			if (url_lc.contains("://plates.library.cornell.edu")) {
				urlRelation = "bookplate";
				if (jsonModel.containsKey("description"))
					sfs.add( new SolrField ("donor_t", ((String)jsonModel.get("description")) ));
				sfs.add( new SolrField ("donor_s", url.substring(url.lastIndexOf('/')+1)) );
			} else if (url.toLowerCase().contains("://pda.library.cornell.edu")) {
				urlRelation = "pda";
			}
			if ( ! jsonModel.containsKey("description")) {
				sfs.add( new SolrField ("url_"+urlRelation+"_display",url));						
			} else {
				sfs.add( new SolrField ("url_"+urlRelation+"_display",url + "|" + jsonModel.get("description")));
				sfs.add( new SolrField ("notes_t",((String)jsonModel.get("description"))));
			}
			if (urlRelation.equals("access")) {
				jsonModel.put("url", url);
				ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, jsonModel);
				sfs.add( new SolrField("url_access_json",jsonstream.toString("UTF-8")) );
			}
		}

		return sfs;
	}	
}
