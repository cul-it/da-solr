package edu.cornell.library.integration.marc;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;

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

	public void addControlFieldResultSet( final ResultSet rs ) {
		addControlFieldResultSet(rs,false);
	}
	public void addControlFieldResultSet( final ResultSet rs, boolean nonBreaking ) {
		while (rs.hasNext()) {
			final QuerySolution sol = rs.nextSolution();
			addControlFieldQuerySolution( sol, nonBreaking );
		}

	}

	public void addControlFieldQuerySolution( final QuerySolution sol ) {
		addControlFieldQuerySolution(sol,false);
	}
	public void addControlFieldQuerySolution( final QuerySolution sol, boolean nonBreaking ) {
		final String f_uri = nodeToString( sol.get("field") );
		final Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
		final ControlField f = new ControlField(field_no,
				nodeToString(sol.get("tag")),nodeToString(sol.get("value")));
		if (nonBreaking)
			f.value = f.value.replaceAll(" ", "\u00A0");
		this.controlFields.add(f);
		if (f.tag.equals("001"))
			this.id = f.value;
		else if (f.tag.equals("005"))
			this.modifiedDate = f.value;
	}

	public void addDataFieldResultSet( final ResultSet rs ) {
		while( rs.hasNext() ){
			final QuerySolution sol = rs.nextSolution();
			addDataFieldQuerySolution(sol, null);
		}
	}
	public void addDataFieldResultSet( final ResultSet rs, final String mainTag ) {
		while( rs.hasNext() ){
			final QuerySolution sol = rs.nextSolution();
			addDataFieldQuerySolution(sol,mainTag);
		}
	}

	public void addDataFieldQuerySolution( final QuerySolution sol ) {
		addDataFieldQuerySolution(sol, null);
	}

	public void addDataFieldQuerySolution( final QuerySolution sol, final String mainTag ) {
		final String f_uri = nodeToString( sol.get("field") );
		final Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
		final String sf_uri = nodeToString( sol.get("sfield") );
		final Integer sfield_no = Integer.valueOf( sf_uri.substring( sf_uri.lastIndexOf('_') + 1 ) );
		DataField f = null;
		for (DataField df : this.dataFields)
			if (df.id == field_no)
				f = df;
		if (f == null) {
			f = new DataField();
			f.id = field_no;
			f.tag = nodeToString( sol.get("tag"));
			f.ind1 = nodeToString(sol.get("ind1")).charAt(0);
			f.ind2 = nodeToString(sol.get("ind2")).charAt(0);
			if (sol.contains("p")) {
				final String p = nodeToString(sol.get("p"));
				f.mainTag = p.substring(p.length() - 3);
			} else if (mainTag != null)
				f.mainTag = mainTag;
		}
		final Subfield sf = new Subfield(sfield_no,
				nodeToString( sol.get("code")).charAt(0), nodeToString( sol.get("value")));
		if (sf.code.equals('6')) {
			if ((sf.value.length() >= 6) && Character.isDigit(sf.value.charAt(4))
					&& Character.isDigit(sf.value.charAt(5))) {
				f.linkNumber = Integer.valueOf(sf.value.substring(4, 6));
			}
		}
		f.subfields.add(sf);
		this.dataFields.add(f);

	}

	public Collection<DataFieldSet> matchAndSortDataFields() {
		return matchAndSortDataFields(VernMode.ADAPTIVE);
	}

	public Collection<DataFieldSet> matchAndSortDataFields(final VernMode vernMode) {
		// Put all fields with link occurrence numbers into matchedFields to be grouped by
		// their occurrence numbers. Everything else goes in sorted fields keyed by field id
		// to be displayed in field id order. If vernMode is SINGULAR or SING_VERN, all
		// occurrence numbers are ignored and treated as "01".
		final Map<Integer,DataFieldSet.Builder> matchedFields  = new HashMap<>();
		final Collection<DataFieldSet> sortedFields = new TreeSet<>();

		for( final DataField f: this.dataFields) {

			if (vernMode.equals(VernMode.SING_VERN) || vernMode.equals(VernMode.SINGULAR))
				f.linkNumber = 1;
			if ((f.linkNumber != null) && (f.linkNumber != 0)) {
				DataFieldSet.Builder fsb;
				if (matchedFields.containsKey(f.linkNumber)) {
					fsb = matchedFields.get(f.linkNumber);
					if (fsb.getId() > f.id) fsb.setId(f.id);
				} else {
					fsb = new DataFieldSet.Builder().setLinkNumber(f.linkNumber).setId(f.id).setMainTag(f.mainTag);
				}
				fsb.addToFields(f);
				matchedFields.put(fsb.getLinkNumber(), fsb);
			} else {
				DataFieldSet fs = new DataFieldSet.Builder().setId(f.id).setMainTag(f.mainTag).addToFields(f).build();
				sortedFields.add(fs);
			}
		}
		// Take groups linked by occurrence number, and add them as groups to the sorted fields
		// keyed by the smallest field id of the group. Groups will be added together, but with
		// that highest precedence of the lowest field id.
		for( final Integer linkNumber : matchedFields.keySet() ) {
			final DataFieldSet fs = matchedFields.get(linkNumber).build();
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
