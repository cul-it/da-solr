package edu.cornell.library.integration.marc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;
import org.marc4j.marc.VariableField;
import org.marc4j.marc.impl.LeaderImpl;
import org.marc4j.marc.impl.SubfieldImpl;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord implements Comparable<MarcRecord>{

	public final static String MARC_DATE_FORMAT = "yyyyMMddHHmmss";

	public String leader = " ";
	public String modifiedDate = null;
	public TreeSet<ControlField> controlFields = new TreeSet<>();
	public TreeSet<DataField> dataFields = new TreeSet<>();
	public RecordType type;
	public String id;
	public String bib_id;
	public TreeSet<MarcRecord> holdings;

	public MarcRecord( RecordType type ) {
		this.type = type;
		if (type != null && type.equals(RecordType.BIBLIOGRAPHIC))
			holdings = new TreeSet<>();
	}

	public MarcRecord( RecordType type , String marc21OrMarcXml ) throws IOException, XMLStreamException {
		this( type );
		String marcXml = ( marc21OrMarcXml.contains("<record") )
				? marc21OrMarcXml : marcToXml( marc21OrMarcXml );
		try (InputStream is = new ByteArrayInputStream(marcXml.getBytes(StandardCharsets.UTF_8))) {
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = input_factory.createXMLStreamReader(is);
		while (r.hasNext())
			if (r.next() == XMLStreamConstants.START_ELEMENT)
				if (r.getLocalName().equals("record")) {
					processRecord(r);
					break; // We only want one record - ignore additional
				}
		}

	}

	public static List<MarcRecord> getMarcRecords( RecordType type, String marc21OrMarcXml ) throws IOException, XMLStreamException {
		List<MarcRecord> recs = new ArrayList<>();
		String marcXml = ( marc21OrMarcXml.contains("<record>") )
				? marc21OrMarcXml : marcToXml( marc21OrMarcXml );
		try (InputStream is = new ByteArrayInputStream(marcXml.getBytes(StandardCharsets.UTF_8))) {
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader read  = input_factory.createXMLStreamReader(is);
		while (read.hasNext())
			if (read.next() == XMLStreamConstants.START_ELEMENT)
				if (read.getLocalName().equals("record")) {
					MarcRecord rec = new MarcRecord( type );
					rec.processRecord(read);
					recs.add(rec);
				}
		}
		return recs;
	}

	@Override
	public int compareTo(final MarcRecord other) {
		if ( this.type == null ) {
			if ( other.type == null )
				return id.compareTo(other.id);
			return 1;
		}
		if ( ! this.type.equals(other.type) )
			return type.compareTo(other.type);
		return id.compareTo(other.id);
	}

	@Override
    public int hashCode() {
      return this.toString().hashCode();
    }

    @Override
	public boolean equals(final Object o){
		if (this == o) return true;
		if (o == null) return false;
		if (! this.getClass().equals( o.getClass() )) return false;
		MarcRecord other = (MarcRecord) o;
		return Objects.equals(this.type, other.type)
				&& Objects.equals(this.id, other.id);
	}

	@Override
	public String toString( ) {

		final StringBuilder sb = new StringBuilder();
		if ((this.leader != null ) && ! this.leader.equals(""))
			sb.append("000    "+this.leader+"\n");

		for( final ControlField f: this.controlFields) {
			sb.append(f.tag + "    " + f.value+"\n");
		}

		for( final DataField f: this.dataFields) {
			sb.append(f.toStringBuilder());
			sb.append("\n");
		}

		if (this.type.equals(RecordType.BIBLIOGRAPHIC))
			for (MarcRecord holdings : this.holdings)
				sb.append('\n').append(holdings.toString());

		return sb.toString();
	}

	public TreeSet<DataFieldSet> matchAndSortDataFields() {
		return matchAndSortDataFields(false);
	}

	public List<DataField> matchSortAndFlattenDataFields() {
		Collection<DataFieldSet> individualSets = matchAndSortDataFields(false);
		if (individualSets.size() == 1)
			return individualSets.iterator().next().getFields();
		List<DataField> fields = new ArrayList<>();
		for (DataFieldSet fs : individualSets)
			fields.addAll(fs.getFields());
		return fields;
	}

	public List<DataField> matchSortAndFlattenDataFields(String mainTag) {
		Collection<DataFieldSet> individualSets = matchAndSortDataFields(false);
		List<DataField> fields = new ArrayList<>();
		for (DataFieldSet fs : individualSets)
			if (fs.getMainTag().equals(mainTag))
				fields.addAll(fs.getFields());
		return fields;
	}

	public TreeSet<DataFieldSet> matchAndSortDataFields(boolean forceVernMatch) {
		// Put all fields with link occurrence numbers into matchedFields to be grouped by
		// their occurrence numbers. Everything else goes in sorted fields keyed by field id
		// to be displayed in field id order. If forceVernMatch, all occurrence numbers are
		// ignored and treated as "01".
		final Map<String,DataFieldSet.Builder> matchedFields  = new HashMap<>();
		final TreeSet<DataFieldSet> sortedFields = new TreeSet<>();

		for( final DataField f: this.dataFields) {

			if (forceVernMatch)
				f.linkNumber = 1;
			if ((f.linkNumber != null) && (f.linkNumber != 0)) {
				DataFieldSet.Builder fsb;
				String fieldSetKey = f.mainTag+f.linkNumber;
				if (matchedFields.containsKey(fieldSetKey)) {
					fsb = matchedFields.get(fieldSetKey);
					if (fsb.getId() > f.id) fsb.setId(f.id);
				} else {
					fsb = new DataFieldSet.Builder().setLinkNumber(f.linkNumber).setId(f.id).setMainTag(f.mainTag);
				}
				fsb.addToFields(f);
				matchedFields.put(fieldSetKey, fsb);
			} else {
				DataFieldSet fs = new DataFieldSet.Builder().setId(f.id).setMainTag(f.mainTag).addToFields(f).build();
				sortedFields.add(fs);
			}
		}
		// Take groups linked by occurrence number, and add them as groups to the sorted fields
		// keyed by the smallest field id of the group. Groups will be added together, but with
		// that highest precedence of the lowest field id.
		for( final String fieldSetKey : matchedFields.keySet() ) {
			final DataFieldSet fs = matchedFields.get(fieldSetKey).build();
			sortedFields.add(fs);
		}
		return sortedFields;

	}

	public String toString( final String format) {
		if (format == null)
			return this.toString();
		switch (format) {
		case "":
		case "txt":
		case "text":
			return this.toString();
		case "xml":
			return this.toXML();
		default:
			return null;
		}
	}

	public String toXML() {
		try {

			// build XML string
			final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
			final ByteArrayOutputStream xmlstream = new ByteArrayOutputStream();
			final XMLStreamWriter w = outputFactory.createXMLStreamWriter(xmlstream);
			w.writeStartDocument("UTF-8", "1.0");
			w.writeStartElement("record");
			w.writeAttribute("xmlns", "http://www.loc.gov/MARC21/slim");
			w.writeStartElement("leader");
			w.writeCharacters(this.leader);
			w.writeEndElement(); // leader

			for( final ControlField f : this.controlFields) {
				w.writeStartElement("controlfield");
				w.writeAttribute("tag", f.tag);
				w.writeCharacters(f.value);
				w.writeEndElement(); //controlfield
			}

			for( final DataField f : this.dataFields) {
				w.writeStartElement("datafield");
				w.writeAttribute("tag", f.tag);
				w.writeAttribute("ind1", f.ind1.toString());
				w.writeAttribute("ind2", f.ind2.toString());
				for (Subfield sf : f.subfields) {
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
		} catch (final XMLStreamException e) {
			e.printStackTrace();
		} catch (final UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String marcToXml( InputStream is, OutputStream out ) throws IOException {
		boolean returnString = false;
		if (out == null) {
			returnString = true;
			out = new ByteArrayOutputStream();
		}
		MarcPermissiveStreamReader reader = new MarcPermissiveStreamReader(is,true,true);
		MarcXmlWriter writer = new MarcXmlWriter(out, "UTF8", true);
		writer.setUnicodeNormalization(true);
		Record record = null;
		String prevRec = null;
		while (reader.hasNext()) {
			try {
				record = reader.next();
				prevRec = record.getControlFields().get(0).getData();
			} catch (Exception e) {
				System.out.println("Error reading record following #"+prevRec);
				e.printStackTrace();
				continue;
			}
			boolean hasInvalidChars = dealWithBadCharacters(record);
			if (! hasInvalidChars)
				writer.write(record);
		}
		writer.close();
		out.close();
		if (returnString)
			return out.toString();
		return null;
	}

	public static String marcToXml( String marc21 ) throws IOException {
		String xml;
		try ( InputStream in = new ByteArrayInputStream(marc21.getBytes(StandardCharsets.UTF_8)) ) {
			xml = marcToXml( in , null );
		}
		return xml;
	}

    private static final String WEIRD_CHARACTERS = "[\u0001-\u0008\u000b-\u000c\u000e-\u001f]";
    private static final Pattern WEIRD_CHARACTERS_PATTERN = Pattern.compile(WEIRD_CHARACTERS);
    private static final String LN = System.getProperty("line.separator");

	/**
	 * Replace bad characters in a MARC record.
	 * 
	 * @return false if everything went fine,
	 * true if if there are still bad characters after the attempted replacement. 
	 */
	private static boolean dealWithBadCharacters(Record record ) {
		String recordString = record.toString();

		Matcher matcher = WEIRD_CHARACTERS_PATTERN.matcher(recordString);
		if (!matcher.find())
			return false;

		List<Integer> invalidCharsIndex = new ArrayList<>();
		do {
			invalidCharsIndex.add(matcher.start());
		} while (matcher.find());

		StringBuffer badCharLocator = new StringBuffer();
		List<String> invalidChars = new ArrayList<>();
		for (Integer i : invalidCharsIndex) {
			RecordLine line = new RecordLine(recordString, i);

			badCharLocator.append(line.getErrorLocation()).append(LN);
			invalidChars.add(line.getInvalidChar() + " ("
					+ line.getInvalidCharHexa() + ")" + " position: " + i);
			String invalidCharacter = (line.getInvalidChar() + " ("
					+ line.getInvalidCharHexa() + ")" + "position: " + i);
			String badCharacterLocator = (line.getErrorLocation() + LN);

			modifyRecord(record, line, invalidCharacter, badCharacterLocator);

		}

		//check to make sure we dealt with all the weird characters
		matcher = WEIRD_CHARACTERS_PATTERN.matcher(record.toString());
		return matcher.find();
	}

	/**
	 * @param record
	 * @param line
	 * @param invalidCharacter
	 * @param badCharacterLocator
	 */
	private static void modifyRecord(Record record, RecordLine line,
			String invalidCharacter, String badCharacterLocator) {

		// change LEADER
		// String leaderReplaced = "The character is replaced with zero.\n";
		if (line.getLine().startsWith("LEADER")) {
			record.setLeader(new LeaderImpl(record.getLeader().toString()
					.replaceAll(WEIRD_CHARACTERS, "0")));
		}

		// change control fields 
//	       String NonleaderReplaced = "The character is replaced with space.\n";
		if (line.getLine().startsWith("00")) {
			String tag = line.getLine().substring(0, 3);
			org.marc4j.marc.ControlField fd = (org.marc4j.marc.ControlField) record.getVariableField(tag);
			fd.setData(fd.getData().replaceAll(WEIRD_CHARACTERS, " "));
			record.addVariableField(fd);

		// change data fields
		} else if (line.getLine().startsWith("LEADER") == false) {
			String tag = line.getLine().substring(0, 3);
//	        DataField fd = (DataField) record.getVariableField(tag);
			List<VariableField> fds = record.getVariableFields(tag);
			for (VariableField fdv: fds) {
				org.marc4j.marc.DataField fd = (org.marc4j.marc.DataField) fdv;
				record.removeVariableField(fd);

				// indicators
				fd.setIndicator1(String.valueOf(fd.getIndicator1())
						.replaceAll(WEIRD_CHARACTERS, " ").charAt(0));
				fd.setIndicator2(String.valueOf(fd.getIndicator2())
						.replaceAll(WEIRD_CHARACTERS, " ").charAt(0));

				// subfields
				List<org.marc4j.marc.Subfield> sfs = fd.getSubfields();
				List<org.marc4j.marc.Subfield> newSfs = new ArrayList<>();
				List<org.marc4j.marc.Subfield> oldSfs = new ArrayList<>();
				// replace the subfields' weird characters
				for (org.marc4j.marc.Subfield sf : sfs) {
					oldSfs.add(sf);
					char code;
					if (WEIRD_CHARACTERS_PATTERN.matcher(
							String.valueOf(sf.getCode())).find()) {
						code = String.valueOf(sf.getCode())
								.replaceAll(WEIRD_CHARACTERS, " ").charAt(0);
					} else {
						code = sf.getCode();
					}
					newSfs.add(new SubfieldImpl(code, sf.getData().replaceAll(
							WEIRD_CHARACTERS, " ")));
				}
				// remove old subfields ...
				for (org.marc4j.marc.Subfield sf : oldSfs) {
					fd.removeSubfield(sf);
				}
				// ... and add the new ones
				for (org.marc4j.marc.Subfield sf : newSfs) {
					fd.addSubfield(sf);
				}
				record.addVariableField(fd);
			}

		}
	}

	private void processRecord( XMLStreamReader r ) throws XMLStreamException {

		int id = 0;
		while (r.hasNext()) {
			int event = r.next();
			if (event == XMLStreamConstants.END_ELEMENT) {
				if (r.getLocalName().equals("record")) 
					return;
			}
			if (event == XMLStreamConstants.START_ELEMENT) {
				if (r.getLocalName().equals("leader")) {
					this.leader = r.getElementText();
				} else if (r.getLocalName().equals("controlfield")) {
					String tag = null;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag"))
							tag = r.getAttributeValue(i);
					ControlField f = new ControlField(++id,tag,r.getElementText());
					if (f.tag.equals("001"))
						this.id = f.value;
					else if (f.tag.equals("005"))
						this.modifiedDate = f.value;
					this.controlFields.add(f);
				} else if (r.getLocalName().equals("datafield")) {
					DataField f = new DataField();
					f.id = ++id;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag")) {
							f.tag = r.getAttributeValue(i);
							f.mainTag = r.getAttributeValue(i);
						}
						else if (r.getAttributeLocalName(i).equals("ind1"))
							f.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							f.ind2 = r.getAttributeValue(i).charAt(0);
					f.subfields = processSubfields(r);

					for (Subfield sf : f.subfields) if (sf.code.equals('6'))
						if (subfield6Pattern.matcher(sf.value).matches()) {
							if (f.tag.equals("880"))
								f.mainTag = sf.value.substring(0,3);
							f.linkNumber = Integer.valueOf(sf.value.substring(4,6));
							break;
						}
					if (f.mainTag == null)
						f.mainTag = f.tag;
					this.dataFields.add(f);
				}
		
			}
		}
		return;
	}
	private static Pattern subfield6Pattern = Pattern.compile("[0-9]{3}-[0-9]{2}.*");

	private static TreeSet<Subfield> processSubfields( XMLStreamReader r ) throws XMLStreamException {
		TreeSet<Subfield> subfields = new TreeSet<>();
		int id = 0;
		while (r.hasNext()) {
			int event = r.next();
			if (event == XMLStreamConstants.END_ELEMENT)
				if (r.getLocalName().equals("datafield"))
					return subfields;
			if (event == XMLStreamConstants.START_ELEMENT)
				if (r.getLocalName().equals("subfield")) {
					Character code = null;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("code"))
							code = r.getAttributeValue(i).charAt(0);
					if (code == null) code = ' ';
					subfields.add(new Subfield(++id,code,r.getElementText().trim()));
				}
		}
		return subfields; // We should never reach this line.
	}

	public static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}
}
