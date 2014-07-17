package edu.cornell.library.integration.indexer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;

public class IndexAuthorityRecords {

	private VoyagerToSolrConfiguration config;
	private DavService davService;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// load configuration for location of index, location of authorities
		Collection<String> requiredArgs = new HashSet<String>();
		requiredArgs.add("fullAuthXmlDir");
		requiredArgs.add("blacklightSolrUrl");
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
        List<String> authXmlFiles = davService.getFileUrlList(config.getWebdavBaseUrl() + "/" + config.getFullAuthXmlDir());
        Iterator<String> i = authXmlFiles.iterator();
        while (i.hasNext()) {
			String srcFile = i.next();
			InputStream xmlstream = davService.getFileAsInputStream(srcFile);
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlstream);
			processRecords(r);
			xmlstream.close();
        }
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
	
	private static Collection<SolrInputDocument> createSolrDocsFromAuthority( MarcRecord rec ) {
		Collection<SolrInputDocument> docs = new HashSet<SolrInputDocument>();
		Integer id = Integer.valueOf(rec.id);
		String heading = null;
		String headingType = null;
		Collection<String> equivalentValues = new HashSet<String>();
		Collection<String> seeAlsos = new HashSet<String>();
		
		// iterate through fields. Look for main heading and alternate forms.
		Iterator<DataField> i = rec.data_fields.values().iterator();
		while (i.hasNext()) {
			DataField f = i.next();
			if (f.tag.startsWith("1")) {
				// main heading
				heading = f.concatValue();
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
				}
				if (f.tag.equals("150")) {
					headingType = "subject";
					heading = f.concatValue();
				}
			} else if (f.tag.startsWith("4")) {
				// equivalent values
				equivalentValues.add(f.concatValue());
			} else if (f.tag.startsWith("5")) {
				// see alsos
				seeAlsos.add(f.concatValue());
			}
		}
		if ((heading == null) || (headingType == null))
			return docs; // empty collection
		
		SolrInputDocument main = new SolrInputDocument();
		main.addField("id", id);
		main.addField("heading", heading);
		main.addField("headingType", headingType);
		Iterator<String> s = equivalentValues.iterator();
		while (s.hasNext())
			main.addField("equivalentValues", s.next());
		s = seeAlsos.iterator();
		while (s.hasNext())
			main.addField("equivalentValues", s.next());
		docs.add(main);

		int redirCount = 0;
		for (String val: equivalentValues) {
			SolrInputDocument redir = new SolrInputDocument();
			redir.addField("id", id+"_r"+(++redirCount));
			redir.addField("heading",val);
			redir.addField("preferedValue", heading);
			redir.addField("headingType",headingType);
			for (String ev: equivalentValues) {
				if (ev.equals(val)) continue;
				redir.addField("equivalentvalues", ev);
			}
			for (String sa: seeAlsos) {
				redir.addField("seeAlsos", sa);
			}
			docs.add(redir);
		}
		
		for (String val: seeAlsos) {
			SolrInputDocument redir = new SolrInputDocument();
			redir.addField("id", id+"_s"+(++redirCount));
			redir.addField("heading",val);
			redir.addField("headingType",headingType);
			for (String sa: seeAlsos) {
				if (sa.equals(val)) continue;
				redir.addField("seeAlsos", sa);
			}
			docs.add(redir);
		}
		
		return docs;
	}
	
	
	private void insertDocuments( Collection<SolrInputDocument> docs) throws IOException, SolrServerException {
		SolrServer solr = new HttpSolrServer(config.getSolrUrl());
		
		for (SolrInputDocument doc : docs) {
			solr.deleteById((String)doc.getFieldValue("id"));
			solr.add(doc);
		}
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
		public String concatValue() {
			StringBuilder sb = new StringBuilder();
			int sf_id = 0;
			while( this.subfields.containsKey(sf_id+1) ) {
				Subfield sf = this.subfields.get(++sf_id);
				sb.append(sf.toString());
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