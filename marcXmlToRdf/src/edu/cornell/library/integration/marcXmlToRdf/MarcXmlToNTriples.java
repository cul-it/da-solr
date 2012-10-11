package edu.cornell.library.integration.marcXmlToRdf;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.HashSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

public class MarcXmlToNTriples {
	
	public static void marcXmlToNTriples(String xmlfile) throws Exception {
		FileInputStream xmlstream = new FileInputStream( xmlfile );
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = 
				input_factory.createXMLStreamReader(xmlfile, xmlstream);
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					displayRecord(rec);
				}
		}
		xmlstream.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sourcefile = "/users/fbw4/git/integrationLayer/rdf/sources/RadMARCATS1.xml";
		try {
			marcXmlToNTriples( sourcefile );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void displayRecord( MarcRecord rec ) {
		System.out.println("000    "+rec.leader);
	}
	
	public static MarcRecord processRecord( XMLStreamReader r ) throws Exception {
		
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
					rec.control_fields.add(f);
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
				}
		
			}
		}
		return rec;
	}
	
	public static Collection<Subfield> processSubfields( XMLStreamReader r ) throws Exception {
		Collection<Subfield> fields = new HashSet<Subfield>();
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
					fields.add(f);
				}
		}
		return fields; // We should never reach this line.
	}
	
	public final static  String getEventTypeString(int  eventType)
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
		public Collection<ControlField> control_fields = new HashSet<ControlField>();
		public Collection<DataField> data_fields = new HashSet<DataField>();
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
		public Collection<Subfield> subfields;
		
		public Integer equivalance_index;
		public int mapped_field;
	}
	
	static class Subfield {
		
		public int id;
		public Character code;
		public String value;
	}
}
