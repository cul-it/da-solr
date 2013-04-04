package edu.cornell.library.integration.indexer;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord {


		
		public String leader = " ";
		public Map<Integer,ControlField> control_fields 
									= new HashMap<Integer,ControlField>();
		public Map<Integer,DataField> data_fields
									= new HashMap<Integer,DataField>();
		public RecordType type;
		public String id;
		public String bib_id;
		
		public String toString( ) {
			
			StringBuilder sb = new StringBuilder();
			if ((this.leader != null ) && ! this.leader.equals(""))
				sb.append("000    "+this.leader+"\n");
			Integer[] ids = this.control_fields.keySet().toArray(new Integer[ this.control_fields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				ControlField f = this.control_fields.get(id);
				sb.append(f.tag + "    " + f.value+"\n");
			}

			ids = this.data_fields.keySet().toArray(new Integer[ this.data_fields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				DataField f = this.data_fields.get(id);
				sb.append(f.toString());
				sb.append("\n");
			}
			return sb.toString();
		}
		
		public void addControlFieldResultSet( ResultSet rs ) {
			while (rs.hasNext()) {
				QuerySolution sol = rs.nextSolution();
				String f_uri = nodeToString( sol.get("field") );
				Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
				ControlField f = new ControlField();
				f.tag = nodeToString(sol.get("tag"));
				f.value = nodeToString(sol.get("value"));
				f.id = field_no;
				this.control_fields.put(field_no, f);
			}
			
		}
		
		public void addDataFieldResultSet( ResultSet rs ) {
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				String f_uri = nodeToString( sol.get("field") );
				Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
				String sf_uri = nodeToString( sol.get("sfield") );
				Integer sfield_no = Integer.valueOf( sf_uri.substring( sf_uri.lastIndexOf('_') + 1 ) );
				DataField f;
				if (this.data_fields.containsKey(field_no)) {
					f = this.data_fields.get(field_no);
				} else {
					f = new DataField();
					f.id = field_no;
					f.tag = nodeToString( sol.get("tag"));
					f.ind1 = nodeToString(sol.get("ind1")).charAt(0);
					f.ind2 = nodeToString(sol.get("ind2")).charAt(0);
				}
				Subfield sf = new Subfield();
				sf.id = sfield_no;
				sf.code = nodeToString( sol.get("code")).charAt(0);
				sf.value = nodeToString( sol.get("value"));
				if (sf.code.equals('6')) {
					if ((sf.value.length() >= 6) && Character.isDigit(sf.value.charAt(4))
							&& Character.isDigit(sf.value.charAt(5))) {
						f.linkOccurrenceNumber = Integer.valueOf(sf.value.substring(4, 6));
					}
				}
				f.subfields.put(sfield_no, sf);
				this.data_fields.put(field_no, f);
				
			}
		}
		
		public Map<Integer,FieldSet> matchAndSortDataFields() {
			// Put all fields with link occurrence numbers into matchedFields to be grouped by
			// their occurrence numbers. Everything else goes in sorted fields keyed by field id
			// to be displayed in field id order.
			Map<Integer,FieldSet> matchedFields  = new HashMap<Integer,FieldSet>();
			Map<Integer,FieldSet> sortedFields = new HashMap<Integer,FieldSet>();
			Integer[] ids = this.data_fields.keySet().toArray(new Integer[ this.data_fields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				DataField f = this.data_fields.get(id);
				if ((f.linkOccurrenceNumber != null) && (f.linkOccurrenceNumber != 0)) {
					FieldSet fs;
					if (matchedFields.containsKey(f.linkOccurrenceNumber)) {
						fs = matchedFields.get(f.linkOccurrenceNumber);
						if (fs.minFieldNo > f.id) fs.minFieldNo = f.id;
					} else {
						fs = new FieldSet();
						fs.linkOccurrenceNumber = f.linkOccurrenceNumber;
						fs.minFieldNo = f.id;
					}
					fs.fields.add(f);
					matchedFields.put(fs.linkOccurrenceNumber, fs);
				} else {
					FieldSet fs = new FieldSet();
					fs.minFieldNo = f.id;
					fs.fields.add(f);
					sortedFields.put(f.id, fs);
				}
			}
			// Take groups linked by occurrence number, and add them as groups to the sorted fields
			// keyed by the smallest field id of the group. Groups will be added together, but with
			// that highest precendence of the lowest field id.
			for( Integer linkOccurrenceNumber : matchedFields.keySet() ) {
				FieldSet fs = matchedFields.get(linkOccurrenceNumber);
				sortedFields.put(fs.minFieldNo, fs);
			}
			return sortedFields;
			
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
									
				Integer[] ids = this.control_fields.keySet().toArray(new Integer[ this.control_fields.keySet().size() ]);
				Arrays.sort( ids );
				for( Integer id: ids) {
					ControlField f = this.control_fields.get(id);
					w.writeStartElement("controlfield");
					w.writeAttribute("tag", f.tag);
					w.writeCharacters(f.value);
					w.writeEndElement(); //controlfield
				}
				ids = this.data_fields.keySet().toArray(new Integer[ this.data_fields.keySet().size() ]);
				Arrays.sort( ids );
				for( Integer id: ids) {
					DataField f = this.data_fields.get(id);
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
			public Character ind1 = ' ';
			public Character ind2 = ' ';
			public Map<Integer,Subfield> subfields = new HashMap<Integer,Subfield>();
			
			public Integer linkOccurrenceNumber; //from MARC subfield 6

			public String toString() {
				StringBuilder sb = new StringBuilder();
				sb.append(this.tag);
				sb.append(" ");
				sb.append(this.ind1);
				sb.append(this.ind2);
				sb.append(" ");
				
				Integer[] sf_ids = this.subfields.keySet().toArray( new Integer[ this.subfields.keySet().size() ]);
				Arrays.sort(sf_ids);
				Boolean first = true;
				for(Integer sf_id: sf_ids) {
					Subfield sf = this.subfields.get(sf_id);					
					if (first) first = false;
					else sb.append(" ");
					sb.append(sf.value.trim());
				}
				
				return sb.toString();
			}

			public String concateSubfieldsOtherThan6() {
				StringBuilder sb = new StringBuilder();
				
				Integer[] sf_ids = this.subfields.keySet().toArray( new Integer[ this.subfields.keySet().size() ]);
				Arrays.sort(sf_ids);
				Boolean first = true;
				for(Integer sf_id: sf_ids) {
					Subfield sf = this.subfields.get(sf_id);
					if (sf.code.equals('6')) continue;
					
					if (first) first = false;
					else sb.append(" ");
					sb.append(sf.value.trim());
				}
				
				return sb.toString();
			}
			public String concateSpecificSubfields(String subfields) {
				StringBuilder sb = new StringBuilder();
				
				Integer[] sf_ids = this.subfields.keySet().toArray( new Integer[ this.subfields.keySet().size() ]);
				Arrays.sort(sf_ids);
				Boolean first = true;
				for(Integer sf_id: sf_ids) {
					Subfield sf = this.subfields.get(sf_id);
					if (! subfields.contains(sf.code.toString()))
						continue;
					
					if (first) first = false;
					else sb.append(" ");
					sb.append(sf.value.trim());
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
		
		public static class FieldSet {
			Integer minFieldNo;
			Integer linkOccurrenceNumber;
			public Set<DataField> fields = new HashSet<DataField>();
			public String toString() {
				StringBuilder sb = new StringBuilder();
				sb.append(this.fields.size() + "fields / link occurrence number: " + 
				          this.linkOccurrenceNumber +"/ min field no: " + this.minFieldNo);
				Iterator<DataField> i = this.fields.iterator();
				while (i.hasNext()) {
					sb.append(i.next().toString() + "\n");
				}
				return sb.toString();
			}
		}

		
		public static enum RecordType {
			BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
		}
	
	
}
