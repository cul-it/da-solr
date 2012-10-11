package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * processing date result sets into fields pub_date, pub_date_sort, pub_date_display
 * 
 */
public class MARCResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.
		
		//This method needs to return a map of fields:
		Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();		
				
		String leader = "";
		String[] control_fields = new String[25];
		String[][] data_fields = new String[130][];
		
		if (results.containsKey("marc_leader")) {
			ResultSet marc_leader = results.get("marc_leader");
			QuerySolution sol = marc_leader.nextSolution();
			leader = nodeToString( sol.get("l"));
		} else {
			System.out.println("Error: leader should NEVER be missing from a MARC record.");
			return null;
		}
		
		if (results.containsKey("marc_control_fields")) {
			ResultSet marc_control_fields = results.get("marc_control_fields");
			while (marc_control_fields.hasNext()) {
				QuerySolution sol = marc_control_fields.nextSolution();
				String f = nodeToString( sol.get("f") );
				Integer field_no = Integer.valueOf( f.substring( f.lastIndexOf('_') + 1 ) );
				control_fields[field_no] = nodeToString(sol.get("t")) + nodeToString(sol.get("v"));
			}
		}
		
		if (results.containsKey("marc_data_fields")) {
			ResultSet marc_control_fields = results.get("marc_data_fields");
			while (marc_control_fields.hasNext()) {
				QuerySolution sol = marc_control_fields.nextSolution();
				String f = nodeToString( sol.get("f") );
				Integer field_no = Integer.valueOf( f.substring( f.lastIndexOf('_') + 1 ) );

				if (data_fields[field_no] == null) {
					data_fields[field_no] = new String[100];
					data_fields[field_no][0] = nodeToString(sol.get("t")) +
												nodeToString(sol.get("i1")) +
												nodeToString(sol.get("i2"));
				}
				String sf = nodeToString( sol.get("sf"));
				Integer subfield_no = Integer.valueOf( sf.substring( sf.lastIndexOf('_') + 1 ) );
				data_fields[field_no][subfield_no] = nodeToString(sol.get("c")) +
						                             nodeToString(sol.get("v"));
			}
		}
		
		XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
		ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
		XMLStreamWriter w = outputFactory.createXMLStreamWriter(xmlstream);
		w.writeStartDocument("UTF-8", "1.0");
		w.writeStartElement("record");
		w.writeAttribute("xmlns", "http://www.loc.gov/MARC21/slim");
		w.writeStartElement("leader");
		w.writeCharacters(leader);
		w.writeEndElement(); // leader
		for (int i = 1; i < control_fields.length; i++) {
			if (control_fields[i] == null) continue;
			w.writeStartElement("controlfield");
			String tag = control_fields[i].substring(0,3);
			String value = control_fields[i].substring(3);
			w.writeAttribute("tag", tag);
			w.writeCharacters(value);
			w.writeEndElement(); //controlfield
		}
		for (int i = 1; i < data_fields.length; i++) {
			if (data_fields[i] == null) continue;
			w.writeStartElement("datafield");
			String tag = data_fields[i][0].substring(0, 3);
			String ind1 = data_fields[i][0].substring(3, 4);
			String ind2 = data_fields[i][0].substring(4);
			w.writeAttribute("tag", tag);
			w.writeAttribute("ind1", ind1);
			w.writeAttribute("ind2", ind2);
			for (int j = 1; j < data_fields[i].length; j++) {
				if (data_fields[i][j] == null) continue;
				w.writeStartElement("subfield");
				String code = data_fields[i][j].substring(0,1);
				String value = data_fields[i][j].substring(1);
				w.writeAttribute("code", code);
				w.writeCharacters(value);
				w.writeEndElement(); //subfield
			}
			w.writeEndElement(); //datafield
		}
		w.writeEndElement(); // record
		w.writeEndDocument();
		
		String xml = xmlstream.toString("UTF-8");
		addField(fields, "marc_display", xml);
		
		return fields;
	}	

}
