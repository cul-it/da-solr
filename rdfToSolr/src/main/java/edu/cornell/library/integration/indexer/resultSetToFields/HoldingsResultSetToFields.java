package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class HoldingsResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		Map<String,MarcRecord> recs = new HashMap<String,MarcRecord>();
		
		Map<String,Location> locations = new HashMap<String,Location>();
		
		Pattern p = Pattern.compile(".*_([0-9]+)");

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				
				
				if (resultKey.equals("location")) {
/*					StringBuilder sb = new StringBuilder();
					Iterator<String> i = sol.varNames();
					while (i.hasNext()) {
						String f = i.next();
						sb.append(f);
						sb.append(" ");
						sb.append(nodeToString(sol.get(f)));
						sb.append("; ");
					}
					System.out.println(sb.toString());*/
					Location l = new Location();
					l.code = nodeToString(sol.get("code"));
					l.name = nodeToString(sol.get("name"));
					l.library = nodeToString(sol.get("library"));
					Matcher m = p.matcher(nodeToString(sol.get("locuri")));
					if (m.matches()) {
						l.number = Integer.valueOf(m.group(1));
					}
					locations.put(l.code, l);					
					
				} else {
	 				String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (recs.containsKey(recordURI)) {
						rec = recs.get(recordURI);
					} else {
						rec = new MarcRecord();
					}
					if (resultKey.contains("control")) {
						rec.addControlFieldQuerySolution(sol);
					} else if (resultKey.contains("data")) {
						rec.addDataFieldQuerySolution(sol);
					}
					recs.put(recordURI, rec);
					
				}
			}
//			rec.addDataFieldResultSet(rs);
		}
		
		for( String holdingURI: recs.keySet() ) {
			MarcRecord rec = recs.get(holdingURI);
						
			for (ControlField f: rec.control_fields.values()) {
				if (f.tag.equals("001")) {
					rec.id = f.value;
				}
			}
			
			Collection<String> loccodes = new HashSet<String>();
			Collection<String> callnos = new HashSet<String>();
			List<String> holdings = new ArrayList<String>();
			List<String> notes = new ArrayList<String>();
			
			Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
			
			Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				FieldSet fs = sortedFields.get(id);
				DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
				for (DataField f: dataFields) {
					String h = null;
					String i = null;
					for (Subfield sf: f.subfields.values()) {
						if (f.tag.equals("852")) {
							if (sf.code.equals('b')) {
								loccodes.add(sf.value);
							} else if (sf.code.equals('h')) {
								h = sf.value;
							} else if (sf.code.equals('i')) {
								i = sf.value;
							}
							
						} else if (f.tag.equals("866") || f.tag.equals("867") || f.tag.equals("868")) {
							if (sf.code.equals('a')) {
								holdings.add(sf.value);
							}
						} else if (f.tag.startsWith("3") || f.tag.startsWith("5") || f.tag.startsWith("84")) {
							notes.add(f.concateSubfieldsOtherThan6());
						}
					}
					if (h != null) {
						if (i == null) {
							callnos.add(h);
						} else {
							callnos.add(h + " " + i);
						}
					}
				}
			}
			
			
			XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
			ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
			XMLStreamWriter w = outputFactory.createXMLStreamWriter(xmlstream);
			w.writeStartDocument("UTF-8", "1.0");
			w.writeStartElement("record");
			w.writeStartElement("id");
			w.writeCharacters(rec.id);
			w.writeEndElement(); // holdings id
			
			for (String loccode: loccodes) {
				w.writeStartElement("location");
				Location l = locations.get(loccode);
				w.writeStartElement("id");
				w.writeCharacters(l.number.toString());
				w.writeEndElement(); //id
				w.writeStartElement("code");
				w.writeCharacters(l.code);
				w.writeEndElement(); //code
				w.writeStartElement("name");
				w.writeCharacters(l.name);
				w.writeEndElement(); //name
				w.writeStartElement("library");
				w.writeCharacters(l.library);
				w.writeEndElement(); //library
				w.writeEndElement(); //location
			}

			for (String callno: callnos) {
				w.writeStartElement("callno");
				w.writeCharacters(callno);
				w.writeEndElement(); //callno
			}

			for (String holding: holdings) {
				w.writeStartElement("holdings_desc");
				w.writeCharacters(holding);
				w.writeEndElement(); //holdings_desc
			}

			for (String note: notes) {
				w.writeStartElement("note");
				w.writeCharacters(note);
				w.writeEndElement(); //note
			}
			
			w.writeEndElement(); // record
			w.writeEndDocument();
			String xml = xmlstream.toString("UTF-8");
			addField(solrFields,"holdings_record_display",xml);
			
		}
		
		

		
		return solrFields;
	}

	public static class Location {
		public String code;
		public Integer number;
		public String name;
		public String library;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("code: ");
			sb.append(this.code);
			sb.append("; number: ");
			sb.append(this.number);
			sb.append("; name: ");
			sb.append(this.name);
			sb.append("; library: ");
			sb.append(this.library);
			return sb.toString();
		}
	}


}
