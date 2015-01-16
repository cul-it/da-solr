package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeAllPunctuation;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;

public class IndexHeadings {

	private SolrServer solr = null;
	private MessageDigest md = null;
	private Map<String,SolrInputDocument> docs = new HashMap<String,SolrInputDocument>();
	
	private final String PERSNAME = "Personal Name";
	private final String CORPNAME = "Corporate Name";
	private final String EVENT = "Event";
	private final String GENHEAD = "General Heading";
	private final String TOPIC = "Topical Term";
	private final String GEONAME = "Geographic Name";
	private final String CHRONTERM = "Chronological Term";
	private final String GENRE = "Genre/Form Term";
	private final String MEDIUM = "Medium of Performance";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
		requiredArgs.add("blacklightSolrUrl");
		requiredArgs.add("solrUrl");
	            
		VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args,requiredArgs);
		try {
			new IndexHeadings(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	public IndexHeadings(VoyagerToSolrConfiguration config) throws Exception {
        
		solr = new HttpSolrServer(config.getSolrUrl());
		
		URL queryUrl = new URL(config.getBlacklightSolrUrl() + "/terms?terms.fl=author_facet&terms.sort=index&terms.limit=10000000");
		XMLInputFactory inputFactory = XMLInputFactory.newInstance();
		InputStream in = queryUrl.openStream();
		XMLStreamReader r  = inputFactory.createXMLStreamReader(in);

		// fast forward to response body
		FF: while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("lst"))
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name")) {
							String name = r.getAttributeValue(i);
							if (name.equals("terms")) break FF;
						}
			}
		}
		
		// process actual results
		String name = null;
		Integer count = null;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("int")) {
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name"))
							name = r.getAttributeValue(i);
					count = Integer.valueOf(r.getElementText());
					recordCount("author",name,count);
										
					// insert Documents if critical mass
					if (docs.size() > 10_000) {
						System.out.println(name + " => " + count);
						insertDocuments();
					}

				}
			}
		}
		in.close();
		insertDocuments();
	}

	
	private void recordCount(String context, String name, Integer count) throws SolrServerException {
		
		// find or make record for heading
		SolrInputDocument main = getMainSolrDocument(name, context, PERSNAME);
		// TODO: Actual headingTypeDesc needed.
		if (main.containsKey("recordCount")) {
			count += Integer.valueOf(main.getFieldValue("recordCount").toString());
			main.removeField("recordCount");
		}
		main.addField("recordCount", count, 1.0f);
		fileDoc(main);
		
		// find any crossref headings
		
		
	}
	
	private SolrInputDocument getMainSolrDocument(String heading, String headingType, String headingTypeDesc) throws SolrServerException {

		// calculate id for Solr document
		String headingSort = getSortHeading(heading);
		String id = md5Checksum(headingSort + headingType + headingTypeDesc);

		// first check for existing doc in memory
		if (docs.containsKey(id))
			return docs.get(id);
		
		// then check for existing doc in Solr
		SolrQuery query = new SolrQuery();
		query.setQuery("id:"+id);
		SolrDocumentList resultDocs = null;
		try {
			QueryResponse qr = solr.query(query);
			resultDocs = qr.getResults();
		} catch (SolrServerException e) {
			System.out.println("Failed to query Solr." + id);
			System.exit(1);
		}
		SolrInputDocument inputDoc = null;
		if (resultDocs != null) {
			Iterator<SolrDocument> i = resultDocs.iterator();
			while (i.hasNext()) {
				SolrDocument doc = i.next();
				doc.remove("_version_");
				inputDoc = ClientUtils.toSolrInputDocument(doc);
			}
		}
		
		
		// finally, create new doc if not found.
		if (inputDoc == null) {
			inputDoc = new SolrInputDocument();
			inputDoc.addField("heading", heading);
			inputDoc.addField("headingSort", headingSort);
			inputDoc.addField("headingType", headingType);
			inputDoc.addField("headingTypeDesc", headingTypeDesc);
			inputDoc.addField("id", id);
		}
		return inputDoc;
	}
	
	
	private void fileDoc( SolrInputDocument doc ) {
		String id = doc.getFieldValue("id").toString();
		docs.put(id, doc);
	}
	
	private String getSortHeading(String heading) {
		// Remove all punctuation will strip punctuation. We replace hyphens with spaces
		// first so hyphenated words will sort as though the space were present.
		String sortHeading = removeAllPunctuation(heading.
				replaceAll("\\p{InCombiningDiacriticalMarks}+", "").
				toLowerCase().
				replaceAll("-", " "));
		return sortHeading.trim();
	}
	
	
	private void insertDocuments() throws IOException, SolrServerException {
		System.out.println("committing "+docs.size()+" records to Solr.");
		solr.deleteById(new ArrayList<String>(docs.keySet()));
		solr.add(docs.values());
		solr.commit();
		docs.clear();
	}

	
	private String md5Checksum(String s) {
		try {
			if (md == null) md = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("MD5 is dead!!");
			e.printStackTrace();
			return null;
		}
		try {
			md.update(s.getBytes("UTF-8") );
			byte[] digest = md.digest();

			// Create Hex String
	        StringBuffer hexString = new StringBuffer();
	        for (int i=0; i<digest.length; i++)
	            hexString.append(Integer.toHexString(0xFF & digest[i]));
	        return hexString.toString();
		} catch (UnsupportedEncodingException e) {
			System.out.println("UTF-8 is dead!!");
			e.printStackTrace();
			return null;
		}

	}

	private final static String getEventTypeString(int  eventType)
	{
	  switch  (eventType)
	    {
	        case XMLEvent.START_ELEMENT:
	          return "START_ELEMENT";
	        case XMLEvent.END_ELEMENT:
	          return "END_ELEMENT";
	        case XMLEvent.PROCESSING_INSTRUCTION:
	          return "PROCESSING_INSTRUCTION";
	        case XMLEvent.CHARACTERS:
	          return "CHARACTERS";
	        case XMLEvent.COMMENT:
	          return "COMMENT";
	        case XMLEvent.START_DOCUMENT:
	          return "START_DOCUMENT";
	        case XMLEvent.END_DOCUMENT:
	          return "END_DOCUMENT";
	        case XMLEvent.ENTITY_REFERENCE:
	          return "ENTITY_REFERENCE";
	        case XMLEvent.ATTRIBUTE:
	          return "ATTRIBUTE";
	        case XMLEvent.DTD:
	          return "DTD";
	        case XMLEvent.CDATA:
	          return "CDATA";
	        case XMLEvent.SPACE:
	          return "SPACE";
	    }
	  return  "UNKNOWN_EVENT_TYPE ,   "+ eventType;
	}

}
