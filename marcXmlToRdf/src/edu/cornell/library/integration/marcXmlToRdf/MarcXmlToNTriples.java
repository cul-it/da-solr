package edu.cornell.library.integration.marcXmlToRdf;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

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
//					System.out.println(rec.toString());
					mapNonRomanFieldsToRomanizedFields(rec);
				}
		}
		xmlstream.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sourcefile = "/users/fbw4/voyager-harvest/data/fulldump/bib.106_30000.xml";
		try {
			marcXmlToNTriples( sourcefile );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void mapNonRomanFieldsToRomanizedFields( MarcRecord rec ) {
		Map<Integer,Integer> eighteighties = new HashMap<Integer,Integer>();
		Map<Integer,Integer> others = new HashMap<Integer,Integer>();
		String bib_id = rec.control_fields.get(1).value;

		for ( int id: rec.data_fields.keySet() ) {
			DataField f = rec.data_fields.get(id);
			for ( int sf_id: f.subfields.keySet() ) {
				Subfield sf = f.subfields.get(sf_id);
				if (sf.code.equals('6')) {
					int n = Integer.valueOf(sf.value.substring(4, 6));
					if (f.tag.equals("880")) {
						if (eighteighties.containsKey(n)) {
							System.out.println("Error: (bib_id:" + bib_id + ") More than one 880 with the same link index.");
						}
						eighteighties.put(n, id);
					} else {
						if (others.containsKey(n)) {
							System.out.println("Error: (bib_id:" + bib_id + ") More than one field linking to 880s with the same link index.");
						}
						others.put(n, id);
					}
				}
			}
		}
		
		for( int link_id: others.keySet() ) {
			if (eighteighties.containsKey(link_id)) {
				// LINK FOUND
				rec.data_fields.get(others.get(link_id)).mapped_field = eighteighties.get(link_id);
				rec.data_fields.get(eighteighties.get(link_id)).mapped_field = others.get(link_id);
			} else {
				System.out.println("Error: (bib_id:" + bib_id + ") Field linking to non-existant 880.");
			}
		}
		for ( int link_id: eighteighties.keySet() )
			if ( ! others.containsKey(link_id))
				System.out.println("Error: (bib_id:" + bib_id + ") 880 field linking to non-existant main field.");
		
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
	
	public static Map<Integer,Subfield> processSubfields( XMLStreamReader r ) throws Exception {
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
		public Map<Integer,ControlField> control_fields 
									= new HashMap<Integer,ControlField>();
		public Map<Integer,DataField> data_fields
									= new HashMap<Integer,DataField>();
		
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
				sb.append(f.tag);
				sb.append(" ");
				sb.append(f.ind1);
				sb.append(f.ind2);
				sb.append(" ");
				int sf_id = 0;
				while( f.subfields.containsKey(sf_id+1) ) {
					Subfield sf = f.subfields.get(++sf_id);
					sb.append("|");
					sb.append(sf.code);
					sb.append(" ");
					sb.append(sf.value);
				}
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

		public int mapped_field;
	}
	
	static class Subfield {
		
		public int id;
		public Character code;
		public String value;
	}
}
