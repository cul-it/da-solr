package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.RLE_openRTL;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord {


		
		public String leader = " ";
		public String modified_date = null;
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
				addControlFieldQuerySolution( sol );
			}
			
		}
		
		public void addControlFieldQuerySolution( QuerySolution sol ) {
			String f_uri = nodeToString( sol.get("field") );
			Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
			ControlField f = new ControlField();
			f.tag = nodeToString(sol.get("tag"));
			f.value = nodeToString(sol.get("value"));
			f.id = field_no;
			this.control_fields.put(field_no, f);
			if (f.tag.equals("001"))
				this.id = f.value;
			else if (f.tag.equals("005"))
				this.modified_date = f.value;
		}
		
		public void addDataFieldResultSet( ResultSet rs ) {
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				addDataFieldQuerySolution(sol, null);
			}
		}
		public void addDataFieldResultSet( ResultSet rs, String mainTag ) {
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				addDataFieldQuerySolution(sol,mainTag);
			}
		}
		
		public void addDataFieldQuerySolution( QuerySolution sol ) {
			addDataFieldQuerySolution(sol, null);
		}

		public void addDataFieldQuerySolution( QuerySolution sol, String mainTag ) {
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
				if (sol.contains("p")) {
					String p = nodeToString(sol.get("p"));
					f.mainTag = p.substring(p.length() - 3);
				} else if (mainTag != null)
					f.mainTag = mainTag;
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
		
		public Map<Integer,FieldSet> matchAndSortDataFields() {
			return matchAndSortDataFields(VernMode.ADAPTIVE);
		}
		
		public Map<Integer,FieldSet> matchAndSortDataFields(VernMode vernMode) {
			// Put all fields with link occurrence numbers into matchedFields to be grouped by
			// their occurrence numbers. Everything else goes in sorted fields keyed by field id
			// to be displayed in field id order. If vernMode is SINGULAR or SING_VERN, all
			// occurrence numbers are ignored and treated as "01".
			Map<Integer,FieldSet> matchedFields  = new HashMap<Integer,FieldSet>();
			Map<Integer,FieldSet> sortedFields = new HashMap<Integer,FieldSet>();
			Integer[] ids = this.data_fields.keySet().toArray(new Integer[ this.data_fields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				DataField f = this.data_fields.get(id);
				if (vernMode.equals(VernMode.SING_VERN) || vernMode.equals(VernMode.SINGULAR))
					f.linkOccurrenceNumber = 1;
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
			// that highest precedence of the lowest field id.
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
			public String alttag; //subfield 6 tag number for an 880 field
			public Character ind1 = ' ';
			public Character ind2 = ' ';
			public Map<Integer,Subfield> subfields = new HashMap<Integer,Subfield>();
			
			public Integer linkOccurrenceNumber; //from MARC subfield 6
			public String mainTag = null;
				
			public String toString() {
				return this.toString('\u2021');
			}
			
			public String toString(Character subfieldSeparator) {
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
					sb.append(subfieldSeparator);
					sb.append(sf.code);
					sb.append(" ");
					sb.append(sf.value.trim());
				}
				return sb.toString();
			}

			public String concatenateSubfieldsOtherThan6() {
				return concatenateSubfieldsOtherThan("6");
			}

			public String concatenateSubfieldsOtherThan(String unwantedSubfields) {
				StringBuilder sb = new StringBuilder();
				
				Integer[] sf_ids = this.subfields.keySet().toArray( new Integer[ this.subfields.keySet().size() ]);
				Arrays.sort(sf_ids);
				Boolean first = true;
				Boolean rtl = false;
				for(Integer sf_id: sf_ids) {
					Subfield sf = this.subfields.get(sf_id);
					if (sf.code.equals('6'))
						if (sf.value.endsWith("/r"))
							rtl = true;
					if (unwantedSubfields.contains(sf.code.toString()))
						continue;
					
					if (first) first = false;
					else sb.append(" ");
					sb.append(sf.value.trim());
				}
				String val = sb.toString().trim();
				if (rtl && (val.length() > 0)) {
					return RLE_openRTL+val+PDF_closeRTL;
				} else {
					return val;
				}
			}
			public String concatenateSpecificSubfields(String subfields) {
				return concatenateSpecificSubfields(" ",subfields);
			}
			public String concatenateSpecificSubfields(String separator,String subfields) {
				StringBuilder sb = new StringBuilder();
				
				Integer[] sf_ids = this.subfields.keySet().toArray( new Integer[ this.subfields.keySet().size() ]);
				Arrays.sort(sf_ids);
				Boolean first = true;
				Boolean rtl = false;
				for(Integer sf_id: sf_ids) {
					Subfield sf = this.subfields.get(sf_id);
					if (sf.code.equals('6'))
						if (sf.value.endsWith("/r"))
							rtl = true;
					if (! subfields.contains(sf.code.toString()))
						continue;
					
					if (first) first = false;
					else sb.append(separator);
					sb.append(sf.value.trim());
				}
				
				String val = sb.toString().trim();
				if (rtl && (val.length() > 0)) {
					return RLE_openRTL+val+PDF_closeRTL;
				} else {
//					return "Roman";
					return val;
				}
			}
			/**
			 * Returns a list of Subfield values for the DataField, matching the
			 * list of specified subfield codes. Each subfield value will be separated
			 * trim()'d, and any values then empty will be omitted.
			 * 
			 * For right-to-left languages, the start and stop RTL encoding Unicode
			 * markers will be added to either end up each string, as long as the field
			 * has a subfield $6 value ending with "/r". It is assumed that the subfield
			 * $6 will be the first subfield, so the addition of the Unicode markers
			 * will fail for RTL Subfields appearing before the appropriate subfield $6.
			 */
			public List<String> valueListForSpecificSubfields(String subfields) {
				List<String> l = new ArrayList<String>();
				Boolean rtl = false;
				for (Subfield sf : this.subfields.values()) {
					if (sf.code.equals('6'))
						if (sf.value.equals("/r"))
							rtl = true;
					if (subfields.contains(sf.code.toString())) {
						String val = sf.value.trim();
						if (val.length() == 0)
							continue;
						if (rtl)
							l.add(RLE_openRTL+val+PDF_closeRTL);
						else
							l.add(val);
					}
				}
				return l;
			}
			
			public Script getScript() {
				for (Subfield sf: this.subfields.values()) {
					if (sf.code == '6') {
						if (sf.value.endsWith("/(3") || sf.value.endsWith("/(3/r"))
							return Script.ARABIC;
						else if (sf.value.endsWith("/(B"))
							return Script.LATIN;
						else if (sf.value.endsWith("/$1"))
							return Script.CJK;
						else if (sf.value.endsWith("/(N"))
							return Script.CYRILLIC;
						else if (sf.value.endsWith("/S"))
							return Script.GREEK;
						else if (sf.value.endsWith("/(2") || sf.value.endsWith("/(2/r"))
							return Script.HEBREW;
					}
				}
				return Script.UNKNOWN;
			}

			/**
			 * 
			 * @param fulltitle - the already concatenated title from this field
			 * @return string without article if conditions are met to remove one, original title otherwise.
			 * 
			 * We want the full title passed back into the method for the
			 * sake of execution efficiency. If the argument were the list of subfields
			 * to be included in the full title, they would have to be concatenated in 
			 * this method, while they all but guaranteed to be concatenated separately
			 * so that the calling method can have access to the title WITH the article.
			 */
			public String getStringWithoutInitialArticle(String fulltitle) {
				Character nonFilingCharInd = null;
				switch (this.mainTag) {
				case "130":
				case "730":
				case "740":
					nonFilingCharInd = this.ind1;
					break;
				case "222":
				case "240":
				case "242":
				case "243":
				case "245":
				case "830":
					nonFilingCharInd = this.ind2;
				}
				if (nonFilingCharInd == null)
					return fulltitle;
				if (Character.isDigit(nonFilingCharInd)) {
					int nonFilingCharCount = Character.digit(nonFilingCharInd, 10);
					if (nonFilingCharCount > 0 && nonFilingCharCount < fulltitle.length())
						return fulltitle.substring(nonFilingCharCount);
				}
				return fulltitle;
			}
		
		}
		
		public static class Subfield {
			
			public int id;
			public Character code;
			public String value;

			public String toString() {
				return this.toString('\u2021');
			}

			public String toString(Character subFieldSeparator) {
				StringBuilder sb = new StringBuilder();
				sb.append(subFieldSeparator);
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
		
		public static enum Script {
			ARABIC, LATIN, CJK, CYRILLIC, GREEK, HEBREW, UNKNOWN
		}
	
	
}