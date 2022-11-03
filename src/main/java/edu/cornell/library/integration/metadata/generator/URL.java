package edu.cornell.library.integration.metadata.generator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.folio.OkapiClient;
import edu.cornell.library.integration.folio.ReferenceData;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.metadata.support.SupportReferenceData;
import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.SolrFields;
import edu.cornell.library.integration.utilities.SolrFields.SolrField;

/**
 * Process 856 fields from both bibliographic and holdings fields into various URL Solr fields.
 * 
 */
public class URL implements SolrFieldGenerator {

	private static ObjectMapper mapper = new ObjectMapper();

	@Override
	public String getVersion() { return "1.5"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("856","899","holdings"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord bibRec, Config config )
			throws IOException {
		SolrFields sfs = new SolrFields();

		boolean isOnline = false;
		boolean isPrint = false;

		if ( bibRec.marcHoldings != null )
			for (MarcRecord holdingsRec : bibRec.marcHoldings)
				for (DataField f : holdingsRec.dataFields)
					if (f.tag.equals("852"))
						for (Subfield sf : f.subfields)
							if (sf.code.equals('b'))
								if (sf.value.equals("serv,remo"))
									isOnline = true;
								else
									isPrint = true;
		if ( bibRec.folioHoldings != null )
			for (Map<String,Object> holding : bibRec.folioHoldings)
				if ( holding.containsKey("permanentLocationId") ) {
					initLocationsIfNull(config);
					if (folioLocations.getUuid("serv,remo").equals(holding.get("permanentLocationId").toString()))
						isOnline = true;
					else
						isPrint = true;
				}


		Integer userLimit = null;
		for ( DataField f : bibRec.dataFields ) if ( f.mainTag.equals("899") ) {
			Matcher m = userLimit899.matcher(f.concatenateSpecificSubfields("a"));
			if ( m.matches() ) userLimit = Integer.valueOf( m.group(1) );
		}

		List<Map<String,Object>> links = new ArrayList<>();

		List<DataField> allLinkFields = bibRec.matchSortAndFlattenDataFields("856");
		if ( bibRec.marcHoldings != null) for (MarcRecord holdingsRec : bibRec.marcHoldings)
			allLinkFields.addAll(holdingsRec.matchSortAndFlattenDataFields("856"));
		if (bibRec.folioHoldings != null)
			for (Map<String,Object> holding: bibRec.folioHoldings)
				links.addAll(extractLinks(holding,isOnline));

		for (DataField f : allLinkFields) {
			Map<String,Object> processedLink = new HashMap<>();

			List<String> urls = f.valueListForSpecificSubfields("u");
			List<String> instructions = f.valueListForSpecificSubfields("i");
			String linkLabel = f.concatenateSpecificSubfields("3yz");

			for (String instruction : instructions)
			if (instruction.contains("dbcode") || instruction.contains("providercode")) {
				String[] codes = instruction.split(";\\s*");
				for (String code : codes) {
					String[] parts = code.split("=",2);
					if (parts.length == 2 && ! parts[1].equals("?")) {
						String field = parts[0].toLowerCase();
						if ( field.equals("dbcode") || field.equals("providercode") || field.equals("ssid") )
							processedLink.put(field, parts[1]);
						else
							System.out.printf("Unexpected field in b%s 856$i: %s\n",bibRec.id,field);
					}
				}
			} else if (number.matcher(instruction).matches() ) {
				processedLink.put("titleid",instruction);
				sfs.add(new SolrField("ebsco_title_facet",instruction));
			}

			if (urls.size() > 1)
				System.out.printf("b%s 856 field has two ‡u values.\n",bibRec.id);
			if (urls.isEmpty()) {
				System.out.printf("b%s 856 field has no ‡u value.\n",bibRec.id);
				continue;
			}
			String url = urls.iterator().next();
			try { // Use Java URI validation to confirm link
				URI uri = new URI(selectivelyUrlEncode(url));
				if ( uri.getHost() == null ) throw new URISyntaxException(url,"No Host in URL");
			} catch (URISyntaxException e) {
				System.out.printf("URISyntaxException %s; Skipping\n",e.getMessage());
				continue;
			}

			processedLink.put("url", url);

			if ( ! linkLabel.isEmpty())
				processedLink.put( "description", String.join(" ",linkLabel) );

			String relation = isOnline? "access" : "other"; //this is a default and may change later
			if (processedLink.containsKey("description")) { 
				String lc = ((String)processedLink.get("description")).toLowerCase();
				if (lc.contains("finding aid"))
					relation = "findingaid";
				if (relation.equals("access") &&
						(! lc.contains("campus access")) &&
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
			} else if (url.toLowerCase().contains("://pda.library.cornell.edu")) {
				relation = "pda";
			}
			processedLink.put("relation", relation);
			if ( userLimit != null )
				processedLink.put("users", userLimit);
			links.add(processedLink);
		}

		if (isOnline) {
			int accessLinkCount = countLinksByType( links, "access" );
			if ( accessLinkCount == 0 ) {
				if ( countLinksByType( links, "other" ) >= 1 )
					reassignOtherLinksToAccess(links);
				else
					isOnline = false;
			}
			
		}

		sfs.addAll(processedLinksToSolrFields(links));

		if (isOnline)
			sfs.add(new SolrField("online","Online"));
		if (isPrint)
			sfs.add(new SolrField("online","At the Library"));

		return sfs;
	}

	static String selectivelyUrlEncode(String url) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < url.length(); i++) {
			char c = url.charAt(i);
			if (replacements.containsKey(c))
				sb.append(replacements.get(c));
			else
				sb.append(c);
		}
		return sb.toString();
	}
	private static String urlEncodedChar(Character c) {
		try {
			return URLEncoder.encode(String.valueOf(c),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return String.valueOf(c);
		}
	}
	static Map<Character,String> replacements = Arrays.asList(' ','"','{','}')
				.stream().collect(Collectors.toMap(c -> c, c -> urlEncodedChar(c) ));

	@Override
	public SolrFields generateNonMarcSolrFields( Map<String,Object> instance, Config config ) throws IOException {
		SolrFields sfs = new SolrFields();
		boolean isOnline = false, isPrint = false;
		List<Map<String,Object>> holdings = null;
		if ( instance.containsKey("holdings") ) {
			holdings = List.class.cast(instance.get("holdings"));
			for (Map<String,Object> holding : holdings) {
				if ( holding.containsKey("permanentLocationId") ) {
					initLocationsIfNull(config);
					if (folioLocations.getUuid("serv,remo").equals(holding.get("permanentLocationId").toString()))
						isOnline = true;
					else
						isPrint = true;
				}
			}
			
		}

		List<Map<String,Object>> links = extractLinks(instance,isOnline);
		if (holdings != null)
			for (Map<String,Object> holding: holdings)
				links.addAll(extractLinks(holding,isOnline));

		if (isOnline) {
			int accessLinkCount = countLinksByType( links, "access" );
			if ( accessLinkCount == 0 ) {
				if ( countLinksByType( links, "other" ) >= 1 )
					reassignOtherLinksToAccess(links);
				else
					isOnline = false;
			}			
		}
		sfs.addAll(processedLinksToSolrFields(links));


		if (isOnline)
			sfs.add(new SolrField("online","Online"));
		if (isPrint)
			sfs.add(new SolrField("online","At the Library"));

		return sfs;
	}

	private static SolrFields processedLinksToSolrFields( List<Map<String,Object>> links ) throws IOException {
		SolrFields sfs = new SolrFields();
		for (Map<String,Object> link : links) {
			String relation = String.class.cast(link.get("relation"));
			String url = String.class.cast(link.get("url"));
			String description = String.class.cast(link.get("description"));
			if (relation.equals("bookplate")) {
				if (link.containsKey("description"))
					sfs.add( new SolrField ("donor_t", description ));
				sfs.add( new SolrField ("donor_s", url.substring(url.lastIndexOf('/')+1)) );
			}
			if (relation.equals("access")) {
				link.remove("relation");
				ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, link);
				if ( link.containsKey("description") )
					sfs.add( new SolrField ("notes_t",description));
				sfs.add( new SolrField("url_access_json",jsonstream.toString("UTF-8")) );
			} else if ( ! link.containsKey("description")) {
				sfs.add( new SolrField ("url_"+relation+"_display",url));						
			} else {
				sfs.add( new SolrField ("url_"+relation+"_display",url + "|" + description));
				sfs.add( new SolrField ("notes_t",description));
			}
		}
		return sfs;

	}

	private static List<Map<String, Object>> extractLinks(Map<String, Object> record, boolean isOnline) {
		List<Map<String, Object>> links = new ArrayList<>();
		if ( ! record.containsKey("electronicAccess") ) return links;
		if ( ! ArrayList.class.isInstance(record.get("electronicAccess"))) return links;
		List<Map<String,String>> rawLinks = ArrayList.class.cast(record.get("electronicAccess"));
		for (Map<String,String> rawLink : rawLinks) {
			if ( rawLink == null
					|| !rawLink.containsKey("uri")
					|| String.class.cast(rawLink.get("uri")).isEmpty())
				continue;
			Map<String,Object> processedLink = new HashMap<>();
			String url = String.class.cast(rawLink.get("uri").trim());

			try { // Use Java URI validation to confirm link
				URI uri = new URI(selectivelyUrlEncode(url));
				if ( uri.getHost() == null ) throw new URISyntaxException(url,"No Host in URL");
			} catch (URISyntaxException e) {
				System.out.printf("URISyntaxException %s; Skipping\n",e.getMessage());
				continue;
			}

			processedLink.put("url",url);
			List<String> linkText = new ArrayList<>();
			for (String field : Arrays.asList("materialsSpecification"/*3*/,"linkText"/*y*/,"publicNote"/*z*/))
				if (rawLink.containsKey(field) && ! String.class.cast(rawLink.get(field)).isEmpty())
					linkText.add(String.class.cast(rawLink.get(field))
							.replaceAll("\\\\n"," ").replaceAll("\\s+"," ").trim());
			if (linkText.size() >= 1)
				processedLink.put("description", String.join(" ", linkText));
			if (processedLink.containsKey("description")
					&& String.class.cast(processedLink.get("description")).contains("findingaid"))
				processedLink.put("relation", "finding aid");
			else if (url.contains("://pda.library.cornell.edu"))
				processedLink.put("relation", "pda");
			else if (url.contains("://plates.library.cornell.edu"))
				processedLink.put("relation", "bookplate");
			else if (isOnline)
				processedLink.put("relation", "access");
			else
				processedLink.put("relation", "other");

			links.add(processedLink);
		}
		return links;
	}


	private static Pattern userLimit899 = Pattern.compile("^.*[A-Za-z_~](\\d+)u$");
	private static Pattern number = Pattern.compile("^\\d+$");

	private static ReferenceData folioLocations = null;

	private static void reassignOtherLinksToAccess(List<Map<String, Object>> allProcessedLinks) {
		for (Map<String,Object> link : allProcessedLinks)
			if ( ((String)link.get("relation")).equals("other") )
				link.put("relation", "access");
	}

	private static int countLinksByType(List<Map<String, Object>> allProcessedLinks, String linkType) {
		int count = 0;
		for (Map<String,Object> link : allProcessedLinks) if (((String)link.get("relation")).equals(linkType)) count++;
		return count;
	}

	private void initLocationsIfNull(Config config) throws IOException {
		if ( folioLocations == null ) {
			folioLocations = SupportReferenceData.locations;
			if (folioLocations == null) {
				OkapiClient folio = config.getOkapi("Folio");
				folioLocations = new ReferenceData( folio,"/locations","code");
			}
		}
	}
}
