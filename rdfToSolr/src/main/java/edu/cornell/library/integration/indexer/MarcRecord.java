package edu.cornell.library.integration.indexer;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord {


		
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

		public String toString( String format) {
			if (format == null)
				return this.toString();
			if (format.equals("") || format.equalsIgnoreCase("txt") || format.equalsIgnoreCase("text"))
				return this.toString();
			if (! format.equalsIgnoreCase("xml"))
				return null;

			try {

			// build XML string
				XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
				ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
				XMLStreamWriter w = outputFactory.createXMLStreamWriter(xmlstream);
				w.writeStartDocument("UTF-8", "1.0");
				w.writeStartElement("record");
				w.writeAttribute("xmlns", "http://www.loc.gov/MARC21/slim");
				w.writeStartElement("leader");
				w.writeCharacters(this.leader);
				w.writeEndElement(); // leader
									
				int id = 0;
				while( this.control_fields.containsKey(id+1) ) {
					ControlField f = this.control_fields.get(++id);
					w.writeStartElement("controlfield");
					w.writeAttribute("tag", f.tag);
					w.writeCharacters(f.value);
					w.writeEndElement(); //controlfield
				}
				while( this.data_fields.containsKey(id+1) ) {
					DataField f = this.data_fields.get(++id);
					w.writeStartElement("datafield");
					w.writeAttribute("tag", f.tag);
					w.writeAttribute("ind1", f.ind1.toString());
					w.writeAttribute("ind2", f.ind2.toString());
					Iterator<Integer> i2 = f.subfields.keySet().iterator();
					while (i2.hasNext()) {
						Subfield sf = f.subfields.get(i2.next());
						w.writeStartElement("subfield");
						w.writeAttribute("code", sf.code.toString());
						w.writeCharacters(sf.value);
						w.writeEndElement(); //subfield
					}
					w.writeEndElement(); //datafield
				}
				w.writeEndElement(); // record
				w.writeEndDocument();
				return xmlstream.toString("UTF-8");
			} catch (XMLStreamException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
		}

		
		public static class ControlField {
			
			public int id;
			public String tag;
			public String value;
		}
		
		public static class DataField {
			
			public int id;
			public String tag;
			public Character ind1;
			public Character ind2;
			public Map<Integer,Subfield> subfields = new HashMap<Integer,Subfield>();

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
		}
		
		public static class Subfield {
			
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
		
		public static enum RecordType {
			BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
		}
	
	
}
