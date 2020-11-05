package edu.cornell.library.integration.metadata.support;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.TreeSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.utilities.Config;

public class Syndetics {

	public static void main(String[] args)
			throws SQLException, FileNotFoundException, IOException, XMLStreamException {
		Collection<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.add("blacklightSolrUrl");
		Config config = Config.loadConfig(requiredArgs);

		//TODO If this goes to production, we'll need a better way to loop in the source files
		File file=new File("C:\\Users\\fbw4\\Documents\\D&A\\tocschg20200920.xml");

		try ( Connection current = config.getDatabaseConnection("Current");
				PreparedStatement insert = current.prepareStatement(
						"INSERT INTO syndeticsData (isbn, marc) values (?,?)");
				BufferedReader reader = new BufferedReader(new FileReader(file))) {
			String line;
			while( (line = reader.readLine()) != null ) {
				if ( ! line.startsWith("<USMARC>")) continue;
				MarcRecord rec = readSyndeticsMarc(line);
				for ( DataField f : rec.dataFields ) if (f.tag.equals("020"))
					for ( Subfield sf : f.subfields ) if (sf.code.equals('a')) {
						insert.setString(1, sf.value);
						insert.setString(2, line);
						insert.addBatch();
						URL url = new URL(config.getBlacklightSolrUrl()
								+"/select?qt=standard&fl=id&wt=csv&q=isbn_t:"+sf.value);
						try(BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()))) {
							String bibId;
							while ((bibId = in.readLine()) != null)
								if ( ! bibId.equals("id") ) System.out.println("affected bib: "+bibId);
						}
					}
				insert.executeBatch();
			}  
		}
	}

	public static MarcRecord readSyndeticsMarc( String xml ) throws XMLStreamException, IOException {
		MarcRecord rec = new MarcRecord ( MarcRecord.RecordType.BIBLIOGRAPHIC );

		try (InputStream is = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))) {
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = input_factory.createXMLStreamReader(is);
		FieldSection currentSection = FieldSection.NONE;
		int id = 0;
		while (r.hasNext())
			if (r.next() == XMLStreamConstants.START_ELEMENT) {
				String localName = r.getLocalName();
				if (localName.equals("Leader")) {
					rec.leader = r.getElementText();
				} else if (localName.equals("VarCFlds")) {
					currentSection = FieldSection.CONTROL;
				} else if (localName.equals("VarDFlds")) {
					currentSection = FieldSection.DATA;
				} else if (localName.startsWith("Fld")) {
					switch ( currentSection ) {
					case CONTROL:
						rec.controlFields.add(new ControlField(++id, localName.substring(3),r.getElementText()));
						break;
					case DATA:
						DataField field = new DataField( ++id, localName.substring(3));
						for ( int i = 0; i < r.getAttributeCount(); i++ )
							if ( r.getAttributeLocalName(i).equals("I1") )
								field.ind1 = getIndicatorValue(r.getAttributeValue(i));
							else if ( r.getAttributeLocalName(i).equals("I2") )
								field.ind2 = getIndicatorValue(r.getAttributeValue(i));
						TreeSet<Subfield> subfields = new TreeSet<>();
						int sfid = 0 ;
						while ( r.hasNext() ) {
							int event = r.next();
							if ( event == XMLStreamConstants.START_ELEMENT )
								subfields.add(new Subfield(++sfid,r.getLocalName().charAt(0),r.getElementText()));
							if ( event == XMLStreamConstants.END_ELEMENT && r.getLocalName().equals(localName) ) {
								field.subfields = subfields;
								rec.dataFields.add(field);
								break;
							}
						}
						break;
					default: break;
					}
				}
			}
		}


		return rec;
	}
	private static Character getIndicatorValue(String val) {
		if ( val.equals("BLANK") ) return ' ';
		if ( val.length() == 1 ) return val.charAt(0);
		System.out.println("Unexpected indicator value: "+val);
		return null;
	}
	private enum FieldSection{ CONTROL, DATA, NONE; }

}
