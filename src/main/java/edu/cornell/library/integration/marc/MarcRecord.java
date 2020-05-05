package edu.cornell.library.integration.marc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

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
			this.holdings = new TreeSet<>();
	}

	@Deprecated
	public MarcRecord( RecordType type , String marcXml ) throws IOException, XMLStreamException {
		this(type, marcXml, true);
	}

	public MarcRecord( RecordType type , String marcXml, boolean trimSubfields ) throws IOException, XMLStreamException {
		this( type );
		try (InputStream is = new ByteArrayInputStream(marcXml.getBytes(StandardCharsets.UTF_8))) {
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = input_factory.createXMLStreamReader(is);
		while (r.hasNext())
			if (r.next() == XMLStreamConstants.START_ELEMENT)
				if (r.getLocalName().equals("record")) {
					processRecord(r, trimSubfields);
					break; // We only want one record - ignore additional
				}
		}

	}

	public MarcRecord( RecordType type , byte[] rawMarc ) {
		this( type );
		this.leader = new String( Arrays.copyOfRange(rawMarc,0,24), StandardCharsets.UTF_8 );
		int dataBaseAddress = Integer.valueOf(new String( Arrays.copyOfRange(rawMarc,12,17) ));
		byte[] directory = Arrays.copyOfRange(rawMarc, 24, dataBaseAddress);
		byte[] data = Arrays.copyOfRange(rawMarc, dataBaseAddress,rawMarc.length+1);
		int directoryPos = 0;
		int fieldId = 1;
		while (directoryPos < directory.length-1) {
			String tag = new String( Arrays.copyOfRange(directory, directoryPos, directoryPos+3));
			int fieldLength = Integer.valueOf(new String( Arrays.copyOfRange(directory,directoryPos+3,directoryPos+7)));
			int fieldStartPos = Integer.valueOf(new String( Arrays.copyOfRange(directory,directoryPos+7,directoryPos+12)));
			byte[] fieldValue =  Arrays.copyOfRange(data, fieldStartPos, fieldStartPos+fieldLength);
			directoryPos += 12;
			if ( tag.startsWith("00") )
				this.controlFields.add( new ControlField( 100*fieldId++, tag, new String(
						Arrays.copyOfRange(fieldValue,0,fieldValue.length-1),StandardCharsets.UTF_8)));
			else {
				char ind1 = (char)fieldValue[0];
				char ind2 = (char)fieldValue[1];
				TreeSet<Subfield> subfields = new TreeSet<>();
				List<Integer> subfieldSeparatorPositions = new ArrayList<>();
				if (fieldValue[2] != (byte)0x1F)
					subfieldSeparatorPositions.add(1);
				for ( int i = 2; i < fieldValue.length ; i++ )
					if ( fieldValue[i] == (byte)0x1E || fieldValue[i] == (byte)0x1F )
						subfieldSeparatorPositions.add(i);
				for ( int i = 0; i < subfieldSeparatorPositions.size() - 1; i++) {
					int startpos = subfieldSeparatorPositions.get(i)+1;
					int endpos = subfieldSeparatorPositions.get(i+1);
					if (startpos >= endpos) continue;
					subfields.add(new Subfield(startpos,(char)fieldValue[startpos],Normalizer.normalize(
							new String( Arrays.copyOfRange(fieldValue, startpos+1, endpos),StandardCharsets.UTF_8)
							.replaceAll("[\n\r]", " "),Normalizer.Form.NFC)));
				}
				this.dataFields.add(new DataField(100*fieldId++,tag,ind1,ind2,subfields));
			}
		}
		F: for ( DataField f : this.dataFields )
			for ( Subfield sf : f.subfields )
				if ( sf.code.equals('6') )
					if (subfield6Pattern.matcher(sf.value).matches()) {
						if (f.tag.equals("880"))
							f.mainTag = sf.value.substring(0,3);
						f.linkNumber = Integer.valueOf(sf.value.substring(4,6));
						continue F;
					}
		for ( ControlField f : this.controlFields )
			if ( f.tag.equals("001") ) {
				this.id = f.value;
				break;
			}
	}

	@Override
	public int compareTo(final MarcRecord other) {
		if ( this.type == null ) {
			if ( other.type == null )
				return this.id.compareTo(other.id);
			return 1;
		}
		if ( ! this.type.equals(other.type) )
			return this.type.compareTo(other.type);
		return this.id.compareTo(other.id);
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
			for (MarcRecord h : this.holdings)
				sb.append('\n').append(h.toString());

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
			w.writeCharacters(cleanInvalidXmlChars(this.leader));
			w.writeEndElement(); // leader

			for( final ControlField f : this.controlFields) {
				w.writeStartElement("controlfield");
				w.writeAttribute("tag", cleanInvalidXmlChars(f.tag));
				w.writeCharacters(cleanInvalidXmlChars(f.value));
				w.writeEndElement(); //controlfield
			}

			for( final DataField f : this.dataFields) {
				w.writeStartElement("datafield");
				w.writeAttribute("tag", cleanInvalidXmlChars(f.tag));
				w.writeAttribute("ind1", cleanInvalidXmlChars(f.ind1.toString()));
				w.writeAttribute("ind2", cleanInvalidXmlChars(f.ind2.toString()));
				for (Subfield sf : f.subfields) {
					w.writeStartElement("subfield");
					w.writeAttribute("code", cleanInvalidXmlChars(sf.code.toString()));
					w.writeCharacters(cleanInvalidXmlChars(sf.value));
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
	private static String cleanInvalidXmlChars(String text) {
		return text.replaceAll("[^\u0009\r\n\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF]", " ");
	}


	private void processRecord( XMLStreamReader r, boolean trimSubfields ) throws XMLStreamException {

		int fid = 0;
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
					ControlField f = new ControlField(++fid,tag,r.getElementText());
					if (f.tag.equals("001"))
						this.id = f.value;
					else if (f.tag.equals("005"))
						this.modifiedDate = f.value;
					this.controlFields.add(f);
				} else if (r.getLocalName().equals("datafield")) {
					DataField f = new DataField();
					f.id = fid += 100;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("tag")) {
							f.tag = r.getAttributeValue(i);
							f.mainTag = r.getAttributeValue(i);
						}
						else if (r.getAttributeLocalName(i).equals("ind1"))
							f.ind1 = r.getAttributeValue(i).charAt(0);
						else if (r.getAttributeLocalName(i).equals("ind2"))
							f.ind2 = r.getAttributeValue(i).charAt(0);
					f.subfields = processSubfields(r, trimSubfields);

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

	private static TreeSet<Subfield> processSubfields( XMLStreamReader r, boolean trimSubfields )
			throws XMLStreamException {
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
					if ( trimSubfields )
						subfields.add(new Subfield(++id,code,r.getElementText().trim()));
					else
						subfields.add(new Subfield(++id,code,r.getElementText()));
				}
		}
		return subfields; // We should never reach this line.
	}

	public static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}
}
