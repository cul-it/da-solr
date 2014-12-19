package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.removeTrailingPunctuation;

public class IndexAuthorityRecords {

	private VoyagerToSolrConfiguration config;
	private DavService davService;
	private SolrServer solr = null;
	private MessageDigest md = null;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("xmlDir");
//		requiredArgs.add("blacklightSolrUrl");
		requiredArgs.add("solrUrl");
		VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args,requiredArgs);
		try {
  //  	   IndexAuthorityRecords iar =
    			   new IndexAuthorityRecords(config);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	public IndexAuthorityRecords(VoyagerToSolrConfiguration config) throws Exception {
		this.config = config;
        this.davService = DavServiceFactory.getDavService(config);
        List<String> authXmlFiles = davService.getFileUrlList(config.getWebdavBaseUrl() + "/" + config.getXmlDir());
        Iterator<String> i = authXmlFiles.iterator();
        while (i.hasNext()) {
			String srcFile = i.next();
			System.out.println(srcFile);
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			processRecords(r);
			xmlstream.close();
        }
	}
	
	private SolrInputDocument getSolrDocument( String heading,String headingType, String headingTypeDesc ) throws SolrServerException {
		String concat = heading + headingType + headingTypeDesc;
		try {
			if (md == null) md = java.security.MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("MD5 is dead!!");
			e.printStackTrace();
			return null;
		}
		String id = null;
		try {
			md.update(concat.getBytes("UTF-8") );
			byte[] digest = md.digest();

			// Create Hex String
	        StringBuffer hexString = new StringBuffer();
	        for (int i=0; i<digest.length; i++)
	            hexString.append(Integer.toHexString(0xFF & digest[i]));
	        id = hexString.toString();
		} catch (UnsupportedEncodingException e) {
			System.out.println("UTF-8 is dead!!");
			e.printStackTrace();
			return null;
		}
		
		if (solr == null) solr = new HttpSolrServer(config.getSolrUrl());
	//	solr.setParser(new XMLResponseParser());
		
		SolrQuery query = new SolrQuery();
		query.setQuery("id:"+id);
		SolrDocumentList docs = solr.query(query).getResults();
		Iterator<SolrDocument> i = docs.iterator();
		SolrInputDocument inputDoc = null;
		while (i.hasNext()) {
			SolrDocument doc = i.next();
			doc.remove("_version_");
			inputDoc = ClientUtils.toSolrInputDocument(doc);
		}
		if (inputDoc == null) {
			inputDoc = new SolrInputDocument();
			inputDoc.addField("heading", heading);
			inputDoc.addField("headingType", headingType);
			inputDoc.addField("headingTypeDesc", headingTypeDesc);
			inputDoc.addField("id", id);
//		} else {
//			System.out.println("existing record found for "+id+" ("+heading+")");
		}
		return inputDoc;
	}

	private void processRecords (XMLStreamReader r) throws Exception {
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					Collection<SolrInputDocument> solrdocs = createSolrDocsFromAuthority(rec);
					insertDocuments(solrdocs);
				}
		}
	}
	
	private Collection<SolrInputDocument> createSolrDocsFromAuthority( MarcRecord rec ) throws SolrServerException {
		Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
		String heading = null;
		String headingType = null;
		String headingTypeDesc = null;
		Collection<Relation> sees = new HashSet<Relation>();
		Collection<Relation> seeAlsos = new HashSet<Relation>();
		Collection<String> expectedNotes = new HashSet<String>();
		Collection<String> foundNotes = new HashSet<String>();
		Collection<String> notes = new HashSet<String>();
		
		for (ControlField f : rec.control_fields.values()) {
			if (f.tag.equals("008")) {
				if (f.value.length() >= 10) {
					Character recordType = f.value.charAt(9);
					if (recordType.equals('b')) 
						expectedNotes.add("666");
					else if (recordType.equals('c'))
						expectedNotes.add("664");
				}
			}
		}
		// iterate through fields. Look for main heading and alternate forms.
		Iterator<DataField> i = rec.data_fields.values().iterator();
		while (i.hasNext()) {
			DataField f = i.next();
			if (f.tag.startsWith("1")) {
				// main heading
				heading = f.concatValue("");
				if (f.tag.equals("100") || f.tag.equals("110") || f.tag.equals("111")) {
					headingType = "author";
					Iterator<Subfield> j = f.subfields.values().iterator();
					while (j.hasNext()) {
						Subfield sf = j.next();
						if (sf.code.equals('t')) {
							headingType = "authortitle";
							break;
						}
					}
					if (f.tag.equals("100")) {
						headingTypeDesc = "Personal Name";
					} else if (f.tag.equals("110")) {
						headingTypeDesc = "Corporate Name";
					} else {
						headingTypeDesc = "Event";
					}
				}
				if (f.tag.equals("130")) {
					headingType = "authortitle";
					headingTypeDesc = "General Heading";
				}
				if (f.tag.equals("150")) {
					headingType = "subject";
					headingTypeDesc = "Topical Term";
				} else if (f.tag.equals("151")) {
					headingType = "subject";
					headingTypeDesc = "Geographic Name";
				} else if (f.tag.equals("148")) {
					headingType = "subject";
					headingTypeDesc = "Chronological Term";
				} else if (f.tag.equals("155")) {
					headingType = "subject";
					headingTypeDesc = "Genre/Form Term";
				} else if (f.tag.equals("162")) {
					headingType = "subject";
					headingTypeDesc = "Medium of Performance Term";
				} else if (f.tag.equals("180")) {
					headingType = "subject";
					headingTypeDesc = "General Subdivision";
				} else if (f.tag.equals("181")) {
					headingType = "subject";
					headingTypeDesc = "Geographic Subdivision";
				} else if (f.tag.equals("182")) {
					headingType = "subject";
					headingTypeDesc = "Chronological Subdivision";
				} else if (f.tag.equals("185")) {
					headingType = "subject";
					headingTypeDesc = "Genre/Form Subdivision";
				}
			} else if (f.tag.equals("260") || f.tag.equals("360")) {
				notes.add("Search under: "+f.concatValue(""));
			} else if (f.tag.startsWith("4")) {
				// equivalent values
				Relation r = determineRelationship(f);
				expectedNotes.addAll(r.expectedNotes);
				r.heading = f.concatValue("iw");
				sees.add(r);
			} else if (f.tag.startsWith("5")) {
				// see alsos
				Relation r = determineRelationship(f);
				expectedNotes.addAll(r.expectedNotes);
				r.heading = f.concatValue("iw");
				seeAlsos.add(r);
			} else if (f.tag.equals("663")) {
				foundNotes.add("663");
				if (expectedNotes.contains("663")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 663 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("664")) {
				foundNotes.add("664");
				if (expectedNotes.contains("664")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 664 found, but no matching 4XX or 5XX subfield w or matching record type: c. "+rec.id);
				}
			} else if (f.tag.equals("665")) {
				foundNotes.add("665");
				if (expectedNotes.contains("665")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 665 found, but no matching 4XX or 5XX subfield w. "+rec.id);
				}
			} else if (f.tag.equals("666")) {
				foundNotes.add("666");
				if (expectedNotes.contains("666")) {
					notes.add(f.concatValue(""));
				} else {
					System.out.println("Field 666 found, but no matching record type: b. "+rec.id);
				}
			}
		}
		if (heading == null || headingType == null || headingTypeDesc == null) {
			System.out.println("Not deriving heading browse entries from record. "+rec.id);
			return docs; // empty collection
		}
		
		SolrInputDocument main = getSolrDocument(heading, headingType, headingTypeDesc);
		main.addField("marcId",rec.id,1.0f);
		//Never setting mainEntry to false, so if mainEntry is populated it's already true.
		if ( ! main.containsKey("mainEntry")) 
			main.addField("mainEntry", true, 1.0f);
		for (String note : notes)
			main.addField("notes",note,1.0f);
		docs.add(main);
		
		if (headingType.equals("author")) {
			main = getSolrDocument(heading, "subject", headingTypeDesc);
			main.addField("marcId",rec.id,1.0f);
			//Never setting mainEntry to false, so if mainEntry is populated it's already true.
			if ( ! main.containsKey("mainEntry")) 
				main.addField("mainEntry", true, 1.0f);
			for (String note : notes)
				main.addField("notes",note,1.0f);
			docs.add(main);
		}
		
		expectedNotes.removeAll(foundNotes);
		if ( ! expectedNotes.isEmpty())
			System.out.println("Expected notes based on 4XX and/or 5XX subfield ws that didn't appear. "+rec.id);

		for (Relation r: sees) {
			if ( ! r.display) continue;
			if (headingType.equals("author")) {
				if (r.applicableContexts.contains(Applicable.NAME))
					docs.add(crossRef("preferedForm",r,heading,"author",headingTypeDesc,rec.id));
				if (r.applicableContexts.contains(Applicable.SUBJECT))
					docs.add(crossRef("preferedForm",r,heading,"subject",headingTypeDesc,rec.id));
			} else {
				docs.add(crossRef("preferedForm",r,heading,headingType,headingTypeDesc,rec.id));
			}
		}
		
		for (Relation r: seeAlsos) {
			if ( ! r.display) continue;
			if (headingType.equals("author")) {
				if (r.applicableContexts.contains(Applicable.NAME))
					docs.add(crossRef("seeAlso",r,heading,"author",headingTypeDesc,rec.id));
				if (r.applicableContexts.contains(Applicable.SUBJECT))
					docs.add(crossRef("seeAlso",r,heading,"subject",headingTypeDesc,rec.id));
			} else {
				docs.add(crossRef("seeAlso",r,heading,headingType,headingTypeDesc,rec.id));
			}
		}
		
		return docs;
	}
	
	SolrInputDocument crossRef(String crossRefType, Relation r,
			String heading, String headingType, String headingTypeDesc, String marcId) throws SolrServerException {
		SolrInputDocument redir = getSolrDocument(r.heading, headingType, headingTypeDesc);
		if (r.relationship == null)
			redir.addField(crossRefType, heading,1.0f);
		else 
			redir.addField(crossRefType, heading + "|" + r.relationship, 1.0f);
		redir.addField("marcId",marcId,1.0f);
		return redir;
		
	}
	
	private Relation determineRelationship( DataField f ) {
		// Is there a subfield w? The relationship note in subfield w
		// describes the 4XX or 5XX heading, and must be reversed for the
		// from tracing.
		Relation r = new Relation();
		for (Subfield sf : f.subfields.values()) {
			if (sf.code.equals('w')) {
				if (sf.value.startsWith("a")) {
					//earlier heading
					r.relationship = "Later Heading";
				} else if (sf.value.startsWith("b")) {
					//later heading
					r.relationship = "Earlier Heading";
				} else if (sf.value.startsWith("d")) {
					//acronym
					r.relationship = "Full Heading";
				} else if (sf.value.startsWith("f")) {
					//musical composition
					r.relationship = "Musical Composition Based on this Work";
				} else if (sf.value.startsWith("g")) {
					//broader term
					r.relationship = "Narrower Term";
				} else if (sf.value.startsWith("h")) {
					//narrower term
					r.relationship = "Broader Term";
				} else if (sf.value.startsWith("i")) {
					// get relationship name from subfield i
				} else if (sf.value.startsWith("r")) {
					// get relationship name from subfield i
					//  also something about subfield 4? Haven't seen one.
				} else if (sf.value.startsWith("t")) {
					//parent body
					r.relationship = "Parent Body";
				}
				
				if (sf.value.length() >= 2) {
					Character offset1 = sf.value.charAt(1);
					if (offset1.equals('a')) {
						r.applicableContexts.add(Applicable.NAME);
					} else if (offset1.equals('b')) {
						r.applicableContexts.add(Applicable.SUBJECT);
					} else if (offset1.equals('c')) {
						r.applicableContexts.add(Applicable.SERIES);
					} else if (offset1.equals('d')) {
						r.applicableContexts.add(Applicable.NAME);
						r.applicableContexts.add(Applicable.SUBJECT);
					} else if (offset1.equals('e')) {
						r.applicableContexts.add(Applicable.NAME);
						r.applicableContexts.add(Applicable.SERIES);
					} else if (offset1.equals('f')) {
						r.applicableContexts.add(Applicable.SUBJECT);
						r.applicableContexts.add(Applicable.SERIES);
					} else { // g (or other)
						r.applicableContexts.add(Applicable.NAME);
						r.applicableContexts.add(Applicable.SUBJECT);
						r.applicableContexts.add(Applicable.SERIES);
					}
				} else {
					r.applicableContexts.add(Applicable.NAME);
					r.applicableContexts.add(Applicable.SUBJECT);
					r.applicableContexts.add(Applicable.SERIES);
				}
				
				if (sf.value.length() >= 3) {
					Character offset2 = sf.value.charAt(2);
					if (offset2.equals('a')) {
						r.relationship = "Later Form of Heading";
					}
				}
				
				if (sf.value.length() >= 4) {
					Character offset3 = sf.value.charAt(3);
					if (offset3.equals('a')) {
						r.display = false;
					} else if (offset3.equals('b')) {
						r.display = false;
						r.expectedNotes.add("664");
					} else if (offset3.equals('c')) {
						r.display = false;
						r.expectedNotes.add("663");
					} else if (offset3.equals('d')) {
						r.display = false;
						r.expectedNotes.add("665");
					}
				}
			} else if (sf.code.equals('i')) {
				r.relationship = removeTrailingPunctuation(sf.value,": ");
			}
		}
		return r;
	}
	
	private static class Relation {	
		public String relationship = null;
		public String heading = null;
		public Collection<Applicable> applicableContexts = new HashSet<Applicable>();
		public Collection<String> expectedNotes = new HashSet<String>();
		boolean display = true;
	}
	private static enum Applicable {
		NAME, SUBJECT, SERIES /*Not currently implementing series header browse*/
	}

	
	private void insertDocuments( Collection<SolrInputDocument> docs) throws IOException, SolrServerException {
		if (solr == null) solr = new HttpSolrServer(config.getSolrUrl());
		for (SolrInputDocument doc : docs) {
			solr.deleteById((String)doc.getFieldValue("id"));
			solr.add(doc);
		}
		solr.commit();
	}

	
	// general MARC methods and classes start here
	
	private MarcRecord processRecord( XMLStreamReader r ) throws Exception {
		
		MarcRecord rec = new MarcRecord();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT")) {
				if (r.getLocalName().equals("record")) 
					return rec;
			}
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("leader")) {
					rec.leader = r.getElementText();
				} else if (r.getLocalName().equals("controlfield")) {
					ControlField f = new ControlField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
					f.value = r.getElementText();
					if (f.tag.equals("001"))
						rec.id = f.value;
					rec.control_fields.put(f.id, f);
				} else if (r.getLocalName().equals("datafield")) {
					DataField f = new DataField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							f.tag = r.getAttributeValue(i);
						else if (r.getAttributeLocalName(i).equals("ind1"))
							f.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							f.ind2 = r.getAttributeValue(i).charAt(0);
					f.subfields = processSubfields(r);
					rec.data_fields.put(f.id, f);
				}
		
			}
		}
		return rec;
	}

	private static Map<Integer,Subfield> processSubfields( XMLStreamReader r ) throws Exception {
		Map<Integer,Subfield> fields = new HashMap<Integer,Subfield>();
		int id = 0;
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT"))
				if (r.getLocalName().equals("datafield"))
					return fields;
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("subfield")) {
					Subfield f = new Subfield();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("code"))
							f.code = r.getAttributeValue(i).charAt(0);
					f.value = r.getElementText();
					fields.put(f.id, f);
				}
		}
		return fields; // We should never reach this line.
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

	
	static class MarcRecord {
		
		public String leader;
		public Map<Integer,ControlField> control_fields 
									= new HashMap<Integer,ControlField>();
		public Map<Integer,DataField> data_fields
									= new HashMap<Integer,DataField>();
		public RecordType type;
		public String id;
		public String bib_id;
		
		public String toString( ) {
			
			StringBuilder sb = new StringBuilder();
			sb.append("000    "+this.leader+"\n");
			int id = 0;
			while( this.control_fields.containsKey(id+1) ) {
				ControlField f = this.control_fields.get(++id);
				sb.append(f.tag + "    " + f.value+"\n");
			}

			while( this.data_fields.containsKey(id+1) ) {
				DataField f = this.data_fields.get(++id);
				sb.append(f.toString());
				sb.append("\n");
			}
			return sb.toString();
		}

	}
	
	static class ControlField {
		
		public int id;
		public String tag;
		public String value;
	}
	
	static class DataField {
		
		public int id;
		public String tag;
		public Character ind1;
		public Character ind2;
		public Map<Integer,Subfield> subfields;

		// Linked field number if field is 880
		public String alttag;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.tag);
			sb.append(" ");
			sb.append(this.ind1);
			sb.append(this.ind2);
			sb.append(" ");
			int sf_id = 0;
			while( this.subfields.containsKey(sf_id+1) ) {
				Subfield sf = this.subfields.get(++sf_id);
				sb.append(sf.toString());
				sb.append(" ");
			}
			return sb.toString();
		}
		public String concatValue(String excludes) {
			StringBuilder sb = new StringBuilder();
			int sf_id = 0;
			while( this.subfields.containsKey(sf_id+1) ) {
				Subfield sf = this.subfields.get(++sf_id);
				if (excludes.contains(sf.code.toString())) continue;
				sb.append(sf.value);
				sb.append(" ");
			}
			return sb.toString().trim();			
		}
	}
	
	static class Subfield {
		
		public int id;
		public Character code;
		public String value;

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("\u2021");
			sb.append(this.code);
			sb.append(" ");
			sb.append(this.value);
			return sb.toString();
		}
	}
	
	static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}
	

}
