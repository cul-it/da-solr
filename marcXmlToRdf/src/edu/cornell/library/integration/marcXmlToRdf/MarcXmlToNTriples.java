package edu.cornell.library.integration.marcXmlToRdf;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

public class MarcXmlToNTriples {
	
	private static String logfile = "xmltordf.log";
	private static BufferedWriter logout;
	
	public static void marcXmlToNTriples(File xmlfile, File targetfile) throws Exception {
		RecordType type ;
		if (xmlfile.getName().startsWith("mfhd"))
			type = RecordType.HOLDINGS;
		else if (xmlfile.getName().startsWith("auth"))
			type = RecordType.AUTHORITY;
		else if (xmlfile.getName().startsWith("bib"))
			type = RecordType.BIBLIOGRAPHIC;
		else { 
			System.out.println("Not processing file. Record type unidentified from filename prefix.");
			return;
		}		
		marcXmlToNTriples( xmlfile, targetfile, type );
	}

	public static void marcXmlToNTriples(File xmlfile, File targetfile, RecordType type) throws Exception {
		FileInputStream xmlstream = new FileInputStream( xmlfile );
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = 
				input_factory.createXMLStreamReader(xmlfile.getPath(), xmlstream);
		BufferedOutputStream out = 
				new BufferedOutputStream(new GZIPOutputStream(
						new FileOutputStream(targetfile)));
		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("START_ELEMENT"))
				if (r.getLocalName().equals("record")) {
					MarcRecord rec = processRecord(r);
					rec.type = type;
					mapNonRomanFieldsToRomanizedFields(rec);
					String ntriples = generateNTriples( rec, type );
					out.write( ntriples.getBytes() );
				}
		}
		xmlstream.close();
		out.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String destdir = "/users/fbw4/voyager-harvest/data/clean";
		File file = new File( "/users/fbw4/voyager-harvest/data/fulldump" );
		File[] files = file.listFiles();
		for (File f: files) {
			String xmlfilename = f.getName();
			File destfile = new File(destdir+File.separator+xmlfilename.replaceAll(".xml$", ".nt.gz"));
			System.out.println(f+" => "+destfile.getPath());
			try {
				marcXmlToNTriples( f, destfile );
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	

	public static String generateNTriples ( MarcRecord rec, RecordType type ) {
		StringBuilder sb = new StringBuilder();
		String id = rec.control_fields.get(1).value;
		String uri_host = "http://fbw4-dev.library.cornell.edu/individuals/";
		String id_pref;
		String record_type_uri;
		if (type == RecordType.BIBLIOGRAPHIC) {
			id_pref = "b";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/BibliographicRecord>";
		} else if (type == RecordType.HOLDINGS) {
			id_pref = "h";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/HoldingsRecord>";
		} else { //if (type == RecordType.AUTHORITY) {
			id_pref = "a";
			record_type_uri = "<http://marcrdf.library.cornell.edu/canonical/0.1/AuthorityRecord>";
		}
		String record_uri = "<"+uri_host+id_pref+id+">";
		sb.append(record_uri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + record_type_uri +" .\n");
		sb.append(record_uri + " <http://www.w3.org/2000/01/rdf-schema#label> \""+id+"\".\n");
		sb.append(record_uri + " <http://marcrdf.library.cornell.edu/canonical/0.1/leader> \""+rec.leader+"\".\n");
		int fid = 0;
		while( rec.control_fields.containsKey(fid+1) ) {
			ControlField f = rec.control_fields.get(++fid);
			String field_uri = "<"+uri_host+id_pref+id+"_"+fid+">";
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> "+field_uri+".\n");
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.tag+"> "+field_uri+".\n");
			sb.append(field_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/ControlField> .\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \""+f.tag+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/value> \""+escapeForNTriples(f.value)+"\".\n");
			if ((f.tag.contentEquals("004")) && (type == RecordType.HOLDINGS)) {
				sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasBibliographicRecord> <"+uri_host+"b"+escapeForNTriples(f.value)+">.\n");
			}
		}
		while( rec.data_fields.containsKey(fid+1) ) {
			DataField f = rec.data_fields.get(++fid);
			String field_uri = "<"+uri_host+"b"+id+"_"+fid+">";
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> "+field_uri+".\n");
			sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.tag+"> "+field_uri+".\n");
			if (f.alttag != null)
				sb.append(record_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasField"+f.alttag+"> "+field_uri+".\n");
			sb.append(field_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/DataField> .\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \""+f.tag+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/ind1> \""+f.ind1+"\".\n");
			sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/ind2> \""+f.ind2+"\".\n");

			int sfid = 0;
			while( f.subfields.containsKey(sfid+1) ) {
				Subfield sf = f.subfields.get(++sfid);
				String subfield_uri = "<"+uri_host+"b"+id+"_"+fid+"_"+sfid+">";
				sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> "+subfield_uri+".\n");
				sb.append(field_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield"+Character.toUpperCase( sf.code )+"> "+subfield_uri+".\n");
				sb.append(subfield_uri+" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://marcrdf.library.cornell.edu/canonical/0.1/Subfield> .\n");
				sb.append(subfield_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/code> \""+sf.code+"\".\n");
				sb.append(subfield_uri+" <http://marcrdf.library.cornell.edu/canonical/0.1/value> \""+escapeForNTriples( sf.value )+"\".\n");
			}

		}

		return sb.toString();
	}
		
	public static String escapeForNTriples( String s ) {
		s = s.replaceAll("\\\\", "\\\\\\\\");
		s = s.replaceAll("\"", "\\\\\\\"");
		s = s.replaceAll("[\n\r]+", "\\\\n");
		s = s.replaceAll("\t","\\\\t");
		return s;
	}
	
	public static void mapNonRomanFieldsToRomanizedFields( MarcRecord rec ) throws Exception {
		Map<Integer,Integer> linkedeighteighties = new HashMap<Integer,Integer>();
		Map<Integer,String> unlinkedeighteighties = new HashMap<Integer,String>();
		Map<Integer,Integer> others = new HashMap<Integer,Integer>();
		String rec_id = rec.control_fields.get(1).value;
		Pattern p = Pattern.compile("^[0-9]{3}.[0-9]{2}.*");
		
		if (logout == null) {
			FileWriter logstream = new FileWriter(logfile);
			logout = new BufferedWriter( logstream );
		}

		for ( int id: rec.data_fields.keySet() ) {
			DataField f = rec.data_fields.get(id);
			for ( int sf_id: f.subfields.keySet() ) {
				Subfield sf = f.subfields.get(sf_id);
				if (sf.code.equals('6')) {
					Matcher m = p.matcher(sf.value);
					if (m.matches()) {
						int n = Integer.valueOf(sf.value.substring(4, 6));
						if (f.tag.equals("880")) {
							if (n == 0) {
								unlinkedeighteighties.put(id, sf.value.substring(0, 3));
							} else {
								if (linkedeighteighties.containsKey(n)) {
									logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") More than one 880 with the same link index.\n");
								}
								linkedeighteighties.put(n, id);
							}
						} else {
							if (others.containsKey(n)) {
								logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") More than one field linking to 880s with the same link index.\n");
							}
							others.put(n, id);
						}
					} else {
						logout.write("Error: ("+rec.type.toString()+":" + rec_id +") "+
								f.tag+" field has ‡6 with unexpected format: \""+sf.value+"\".\n");
					}
				}
			}
		}

		for( int fid: unlinkedeighteighties.keySet() ) {
			rec.data_fields.get(fid).alttag = unlinkedeighteighties.get(fid);
		}
		for( int link_id: others.keySet() ) {
			if (linkedeighteighties.containsKey(link_id)) {
				// LINK FOUND
				rec.data_fields.get(linkedeighteighties.get(link_id)).alttag = rec.data_fields.get(others.get(link_id)).tag;
			} else {
				logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") "+
						rec.data_fields.get(others.get(link_id)).tag+
						" field linking to non-existant 880.\n");
			}
		}
		for ( int link_id: linkedeighteighties.keySet() )
			if ( ! others.containsKey(link_id))
				logout.write("Error: ("+rec.type.toString()+":" + rec_id + ") 880 field linking to non-existant main field.\n");
		logout.flush();
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
		public RecordType type;
		
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

		// Linked field number if field is 880
		public String alttag;
	}
	
	static class Subfield {
		
		public int id;
		public Character code;
		public String value;
	}
	
	static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}
}
