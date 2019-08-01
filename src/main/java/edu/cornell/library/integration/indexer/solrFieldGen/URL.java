package edu.cornell.library.integration.indexer.solrFieldGen;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Process 856 fields from both bibliographic and holdings fields into various URL Solr fields.
 * 
 */
public class URL implements SolrFieldGenerator {

	private static ObjectMapper mapper = new ObjectMapper();

	@Override
	public String getVersion() { return "1.1"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("856","holdings"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord bibRec, Config unused )
			throws IOException {
		SolrFields sfs = new SolrFields();

		boolean isOnline = isOnline(bibRec.holdings);

		List<DataField> allLinkFields = bibRec.matchSortAndFlattenDataFields();
		for (MarcRecord holdingsRec : bibRec.holdings)
			allLinkFields.addAll(holdingsRec.matchSortAndFlattenDataFields("856"));

		List<Map<String,Object>> allProcessedLinks = new ArrayList<>();
		for (DataField f : allLinkFields) {
			Map<String,Object> processedLink = new HashMap<>();

			List<String> urls = f.valueListForSpecificSubfields("u");
			String instructions = f.concatenateSpecificSubfields("i");
			String linkLabel = f.concatenateSpecificSubfields("3yz");

			if (instructions != null &&
					(instructions.contains("dbcode") || instructions.contains("providercode"))) {
				String[] codes = instructions.split(";\\s*");
				for (String code : codes) {
					String[] parts = code.split("=",2);
					if (parts.length == 2 && ! parts[1].equals("?"))
						processedLink.put(parts[0].toLowerCase(), parts[1]);
				}
			}

			if (urls.size() > 1)
				System.out.printf("b%s 856 field has two ‡u values.\n",bibRec.bib_id);
			if (urls.isEmpty()) {
				System.out.printf("b%s 856 field has no ‡u value.\n",bibRec.bib_id);
				continue;
			}
			String url = urls.iterator().next();
			processedLink.put("url", url);

			if ( ! linkLabel.isEmpty())
				processedLink.put( "description", String.join(" ",linkLabel) );

			String relation = isOnline? "access" : "other"; //this is a default and may change later
			if (processedLink.containsKey("description")) { 
				String lc = ((String)processedLink.get("description")).toLowerCase();
				if (lc.contains("finding aid"))
					relation = "findingaid";
				if (relation.equals("access") && 
						(lc.contains("table of contents")
							|| lc.contains("tables of contents")
							|| lc.endsWith(" toc")
							|| lc.contains(" toc ")
							|| lc.startsWith("toc ")
							|| lc.equals("toc")
							|| lc.contains("cover image")
							|| lc.contains("cover art")
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
							|| lc.equals("hathitrust – access limited to full-text search"))) {
					relation = "other";
				}
			}
	
			String url_lc = ((String)processedLink.get("url")).toLowerCase();
			if (url_lc.contains("://plates.library.cornell.edu")) {
				relation = "bookplate";
				if (processedLink.containsKey("description"))
					sfs.add( new SolrField ("donor_t", ((String)processedLink.get("description")) ));
				sfs.add( new SolrField ("donor_s", url.substring(url.lastIndexOf('/')+1)) );
			} else if (url.toLowerCase().contains("://pda.library.cornell.edu")) {
				relation = "pda";
			}
			processedLink.put("relation", relation);

			allProcessedLinks.add(processedLink);

		}
		if (isOnline) {
			int accessLinkCount = countLinksByType( allProcessedLinks, "access" );
			if ( accessLinkCount == 0 ) {
				if ( countLinksByType( allProcessedLinks, "other" ) >= 1 )
					reassignOtherLinksToAccess(allProcessedLinks);
				else
					isOnline = false;
			}
			
		}

		for (Map<String,Object> link : allProcessedLinks) {
			String relation = (String)link.get("relation");
			String url = (String)link.get("url");
			if ( ! link.containsKey("description")) {
				sfs.add( new SolrField ("url_"+relation+"_display",url));						
			} else {
				sfs.add( new SolrField ("url_"+relation+"_display",url + "|" + link.get("description")));
				sfs.add( new SolrField ("notes_t",((String)link.get("description"))));
			}
			if (relation.equals("access")) {
				link.remove("relation");
				ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, link);
				sfs.add( new SolrField("url_access_json",jsonstream.toString("UTF-8")) );
			}
		}
		if (isOnline)
			sfs.add(new SolrField("online","Online"));

		return sfs;
	}

	private static void reassignOtherLinksToAccess(List<Map<String, Object>> allProcessedLinks) {
		for (Map<String,Object> link : allProcessedLinks)
			if ( ((String)link.get("relation")).equals("other") )
				link.put("relation", "access");
	}

	private static int countLinksByType(List<Map<String, Object>> allProcessedLinks, String linkType) {
		int count = 0;
		for (Map<String,Object> link : allProcessedLinks)
			if ( ((String)link.get("relation")).equals(linkType) )
					count++;
		return count;
	}

	private static boolean isOnline(TreeSet<MarcRecord> holdings) {
		for (MarcRecord holdingsRec : holdings)
			for (DataField f : holdingsRec.dataFields)
				if (f.tag.equals("852"))
					for (Subfield sf : f.subfields)
						if (sf.code.equals('b') && sf.value.equals("serv,remo"))
							return true;
		return false;
	}	
}
