package edu.cornell.library.integration.marc;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord implements Comparable<MarcRecord>{

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
	public boolean equals(final MarcRecord other){
		if (other == null) return false;
		if (this.type == null) {
			if (other.type == null)
				return this.id == other.id;
			return false;
		}
		if ( this.type.equals(other.type) )
			return this.id == other.id;
		return false;
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
			sb.append(f.toString());
			sb.append("\n");
		}
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
	public TreeSet<DataFieldSet> matchAndSortDataFields(boolean forceVernMatch) {
		// Put all fields with link occurrence numbers into matchedFields to be grouped by
		// their occurrence numbers. Everything else goes in sorted fields keyed by field id
		// to be displayed in field id order. If vernMode is SINGULAR or SING_VERN, all
		// occurrence numbers are ignored and treated as "01".
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
		if (format.equals("") || format.equalsIgnoreCase("txt") || format.equalsIgnoreCase("text"))
			return this.toString();
		if (! format.equalsIgnoreCase("xml"))
			return null;

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


	public static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}
}
