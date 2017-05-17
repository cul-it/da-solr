package edu.cornell.library.integration.indexer;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.RLE_openRTL;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker.VernMode;
import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.utilities.CharacterSetUtils;

/*
 *  MarcRecord Handler Class
 */
public class MarcRecord {

	public String leader = " ";
	public String modifiedDate = null;
	public TreeSet<ControlField> controlFields = new TreeSet<>();
	public TreeSet<DataField> dataFields = new TreeSet<>();
	public RecordType type;
	public String id;
	public String bib_id;

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

	public Collection<FieldSet> matchAndSortDataFields() {
		return matchAndSortDataFields(VernMode.ADAPTIVE);
	}

	public Collection<FieldSet> matchAndSortDataFields(final VernMode vernMode) {
		// Put all fields with link occurrence numbers into matchedFields to be grouped by
		// their occurrence numbers. Everything else goes in sorted fields keyed by field id
		// to be displayed in field id order. If vernMode is SINGULAR or SING_VERN, all
		// occurrence numbers are ignored and treated as "01".
		final Map<Integer,FieldSet.Builder> matchedFields  = new HashMap<>();
		final Collection<FieldSet> sortedFields = new TreeSet<>();

		for( final DataField f: this.dataFields) {

			if (vernMode.equals(VernMode.SING_VERN) || vernMode.equals(VernMode.SINGULAR))
				f.linkNumber = 1;
			if ((f.linkNumber != null) && (f.linkNumber != 0)) {
				FieldSet.Builder fsb;
				if (matchedFields.containsKey(f.linkNumber)) {
					fsb = matchedFields.get(f.linkNumber);
					if (fsb.id > f.id) fsb.setId(f.id);
				} else {
					fsb = new FieldSet.Builder().setLinkNumber(f.linkNumber).setId(f.id).setMainTag(f.mainTag);
				}
				fsb.addToFields(f);
				matchedFields.put(fsb.linkNumber, fsb);
			} else {
				FieldSet fs = new FieldSet.Builder().setId(f.id).setMainTag(f.mainTag).addToFields(f).build();
				sortedFields.add(fs);
			}
		}
		// Take groups linked by occurrence number, and add them as groups to the sorted fields
		// keyed by the smallest field id of the group. Groups will be added together, but with
		// that highest precedence of the lowest field id.
		for( final Integer linkNumber : matchedFields.keySet() ) {
			final FieldSet fs = matchedFields.get(linkNumber).build();
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


	public static class ControlField implements Comparable<ControlField> {

		public int id;
		public String tag;
		public String value;

		public ControlField( int id, String tag, String value ) {
			this.id = id;
			this.tag = tag;
			this.value = value;
		}
		@Override
		public int compareTo(final ControlField other) {
			return Integer.compare(this.id, other.id);
		}
		public boolean equals( final ControlField other ) {
			if (other == null) return false;
			if (other.id == this.id) return true;
			return false;
		}
	}

	public static class DataField implements Comparable<DataField> {

		public int id;
		public String tag;
		public String alttag; //subfield 6 tag number for an 880 field
		public Character ind1 = ' ';
		public Character ind2 = ' ';
		public TreeSet<Subfield> subfields = new TreeSet<>();

		public Integer linkNumber; //from MARC subfield 6
		public String mainTag = null;

		@Override
		public String toString() {
			return this.toString('\u2021');
		}

		public String toString(final Character subfieldSeparator) {
			final StringBuilder sb = new StringBuilder();
			sb.append(this.tag);
			sb.append(" ");
			sb.append(this.ind1);
			sb.append(this.ind2);

			for(final Subfield sf : this.subfields) {
				sb.append(" ");
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

		public String concatenateSubfieldsOtherThan(final String unwantedSubfields) {
			final StringBuilder sb = new StringBuilder();

			Boolean first = true;
			Boolean rtl = false;
			for(final Subfield sf : this.subfields) {
				if (sf.code.equals('6'))
					if (sf.value.endsWith("/r"))
						rtl = true;
				if (unwantedSubfields.contains(sf.code.toString()))
					continue;

				if (first) first = false;
				else sb.append(" ");
				sb.append(sf.value.trim());
			}
			final String val = sb.toString().trim();
			if (rtl && (val.length() > 0)) {
				return RLE_openRTL+val+PDF_closeRTL;
			}
			return val;
		}
		public String concatenateSpecificSubfields(final String subfields) {
			return concatenateSpecificSubfields(" ",subfields);
		}
		public String concatenateSpecificSubfields(final String separator,final String subfields) {
			final StringBuilder sb = new StringBuilder();

			Boolean first = true;
			Boolean rtl = false;
			for(final Subfield sf : this.subfields) {
				if (sf.code.equals('6'))
					if (sf.value.endsWith("/r"))
						rtl = true;
				if (! subfields.contains(sf.code.toString()))
					continue;

				if (first) first = false;
				else sb.append(separator);
				sb.append(sf.value.trim());
			}

			final String val = sb.toString().trim();
			if (rtl && (val.length() > 0)) {
				return RLE_openRTL+val+PDF_closeRTL;
			}
			return val;
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
		public List<String> valueListForSpecificSubfields(final String subfields) {
			final List<String> l = new ArrayList<>();
			Boolean rtl = false;
			for (final Subfield sf : this.subfields) {
				if (sf.code.equals('6'))
					if (sf.value.equals("/r"))
						rtl = true;
				if (subfields.contains(sf.code.toString())) {
					final String val = sf.value.trim();
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
			for (final Subfield sf: this.subfields) {
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
		public String getStringWithoutInitialArticle(final String fulltitle) {
			final String titlePrefixChars = "[.\"â€œ";

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
				final int nonFilingCharCount = Character.digit(nonFilingCharInd, 10);
				if (nonFilingCharCount > 0 && nonFilingCharCount < fulltitle.length())
					try {
						if (fulltitle.startsWith(RLE_openRTL))
							return RLE_openRTL+
									CharacterSetUtils.stripLeadCharsFromString(
									fulltitle.substring(RLE_openRTL.length()),
									nonFilingCharCount, titlePrefixChars);
						return CharacterSetUtils.stripLeadCharsFromString(
								fulltitle, nonFilingCharCount, titlePrefixChars);
					} catch (IllegalArgumentException e) {
						System.out.println("Initial article not stripped from title: "+
								e.getMessage()+" "+nonFilingCharCount+"/"+fulltitle);
					}
			}
			return fulltitle;
		}

		/**
		 * The field may be an a name, a name and title or just a title depending on the
		 * field type and contents. Extract the value(s).
		 * @return FieldValues
		 */
		public FieldValues getFieldValuesForNameAndOrTitleField(String subfields) {
			char one = this.mainTag.charAt(0), two = this.mainTag.charAt(1), three = this.mainTag.charAt(2);
			if ((two=='0' && three=='0')
					|| (two=='1' && (three=='0'||three=='1')))
				return getFieldValuesForNameMaybeTitleField_x00_x10_x11( subfields );
			if (one=='7'&&(two=='6'||two=='7'||two=='8'))
				return getFieldValuesForTitleMaybeName_76x_77x_78x( subfields );
			throw new IllegalArgumentException( "Method DataField.getFieldValuesForNameAndOrTitleField() "
					+ "called for unsupported field ("+this.mainTag+").");
		}

		/* These fields are primarily title fields, so a title is expected, while name data may
		 * optionally be present.
		 */
		private FieldValues getFieldValuesForTitleMaybeName_76x_77x_78x(String subfields) {
			boolean hasA = false;
			boolean hasDefiniteTitleSubfield = false;
			MeaningOfSubfieldABasedOnSubfield7 seven = MeaningOfSubfieldABasedOnSubfield7.UNK;

			for (Subfield sf : this.subfields)
				switch (sf.code) {
				case 'a':
					hasA = true; break;
				case 'p': case 's': case 't':
					hasDefiniteTitleSubfield = true; break;
				case '7':
					if (sf.value.length() == 0) break;
					switch (sf.value.charAt(0)) {
					case 'p': case 'c': case 'm':
						seven = MeaningOfSubfieldABasedOnSubfield7.AUT; break;
					case 'u':
						seven = MeaningOfSubfieldABasedOnSubfield7.TIT; break;
					default:
						// seven already set to UNK.
					}
				}

			if ( hasA && hasDefiniteTitleSubfield && ! seven.equals(MeaningOfSubfieldABasedOnSubfield7.TIT))
				return getFieldValuesForTitleMaybeName_76x_77x_78x_variantAuthorTitle(subfields);
			return new FieldValues(null,this.concatenateSpecificSubfields(subfields));
		}
		/* This particular linking field entry seems to contain both author and title data. */
		private FieldValues getFieldValuesForTitleMaybeName_76x_77x_78x_variantAuthorTitle(String subfields) {
			List<String> authorSubfields = new ArrayList<>();
			List<String> titleSubfields = new ArrayList<>();
			boolean foundTitle = false;
			for(Subfield sf : this.subfields) {
				if (subfields != null && -1 == subfields.indexOf(sf.code))
					continue;
				if (foundTitle)
					titleSubfields.add(sf.value);
				else
					if (sf.code.equals('t') || sf.code.equals('s') || sf.code.equals('p')) {
						foundTitle = true;
						titleSubfields.add(sf.value);
					} else
						authorSubfields.add(sf.value);
			}
			if ( titleSubfields.isEmpty() )
				return new FieldValues( String.join(" ",authorSubfields));
			return new FieldValues(
					String.join(" ",authorSubfields),
					String.join(" ",titleSubfields));
		}
		private static enum MeaningOfSubfieldABasedOnSubfield7 { UNK,AUT,TIT; }

		/* These fields are primarily name fields, so a name is expected, while title data may
		 * optionally be present.
		 */
		private FieldValues getFieldValuesForNameMaybeTitleField_x00_x10_x11(String subfields) {
			List<String> authorSubfields = new ArrayList<>();
			List<String> titleSubfields = new ArrayList<>();
			boolean foundTitle = false;
			for(Subfield sf : this.subfields) {
				if (subfields != null && -1 == subfields.indexOf(sf.code))
					continue;
				if (foundTitle)
					titleSubfields.add(sf.value);
				else
					if (sf.code.equals('t') || sf.code.equals('k')) {
						foundTitle = true;
						titleSubfields.add(sf.value);
					} else
						authorSubfields.add(sf.value);
			}
			if ( titleSubfields.isEmpty() )
				return new FieldValues( String.join(" ",authorSubfields));
			return new FieldValues(
					String.join(" ",authorSubfields),
					String.join(" ",titleSubfields));
		}

		/* Parse a series of subfields in a single string into a set of Subfield objects */
		private static TreeSet<Subfield> parseSubfields(String subfields, Character subfieldSeparator) {
			String[] values = subfields.split(String.valueOf(subfieldSeparator));
			if (values.length > 1) {
				TreeSet<Subfield> subfieldSet = new TreeSet<>();
				for (int i = 1 ; i < values.length ; i++ ) {
					if (values.length == 0) continue;
					if (values.length == 1)
						subfieldSet.add( new Subfield( i, values[i].charAt(0), "") );
					else
						subfieldSet.add( new Subfield( i, values[i].charAt(0), values[i].substring(1).trim()) );
				}
				return subfieldSet;
			}
			return null;
		}

		@Override
		public int compareTo(final DataField other) {
			return Integer.compare(this.id, other.id);
		}
		public boolean equals( final DataField other ) {
			if (other == null) return false;
			if (other.id == this.id) return true;
			return false;
		}
		public DataField() {}
		public DataField( int id, String tag ) {
			this.id = id;
			this.tag = tag;
			this.mainTag = tag;
		}
		public DataField( int id, String tag, Character ind1, Character ind2, String subfields ) {
			this.id = id;
			this.tag = tag;
			this.ind1 = ind1;
			this.ind2 = ind2;
			this.mainTag = tag;
			this.subfields = parseSubfields(subfields,'‡');
		}
		public DataField( int id, String tag, Character ind1, Character ind2,
				String subfields, Character subfieldSeparator ) {
			this.id = id;
			this.tag = tag;
			this.ind1 = ind1;
			this.ind2 = ind2;
			this.mainTag = tag;
			this.subfields = parseSubfields(subfields,subfieldSeparator);
		}

		public DataField( int id, int linkNumber, String tag ) {
			this.id = id;
			this.linkNumber = linkNumber;
			this.tag = tag;
			this.mainTag = tag;
		}
		public DataField( int id, String tag, Boolean eighteighty ) {
			this.id = id;
			this.tag = (eighteighty)?"880":tag;
			this.mainTag = tag;
		}
		public DataField( int id, int linkNumber, String tag, Boolean eighteighty ) {
			this.id = id;
			this.linkNumber = linkNumber;
			this.tag = (eighteighty)?"880":tag;
			this.mainTag = tag;
		}
		public DataField( int id, int linkNumber, String tag, Boolean eighteighty, TreeSet<Subfield> subfields ) {
			this.id = id;
			this.linkNumber = linkNumber;
			this.tag = (eighteighty)?"880":tag;
			this.mainTag = tag;
			this.subfields = subfields;
		}
		public DataField( int id, int linkNumber, String tag, Boolean eighteighty, Character ind1, Character ind2,
				String subfields ) {
			this.id = id;
			this.linkNumber = linkNumber;
			this.tag = (eighteighty)?"880":tag;
			this.ind1 = ind1;
			this.ind2 = ind2;
			this.mainTag = tag;
			this.subfields = parseSubfields(subfields,'‡');
		}
		public DataField( int id, int linkNumber, String tag, Boolean eighteighty, Character ind1, Character ind2,
				String subfields, Character subfieldSeparator ) {
			this.id = id;
			this.linkNumber = linkNumber;
			this.tag = (eighteighty)?"880":tag;
			this.ind1 = ind1;
			this.ind2 = ind2;
			this.mainTag = tag;
			this.subfields = parseSubfields(subfields,subfieldSeparator);
		}
	}

	public static class Subfield implements Comparable<Subfield> {

		public int id;
		public Character code;
		public String value;

		@Override
		public String toString() {
			return this.toString('\u2021');
		}

		public String toString(final Character subFieldSeparator) {
			final StringBuilder sb = new StringBuilder();
			sb.append(subFieldSeparator);
			sb.append(this.code);
			sb.append(" ");
			sb.append(this.value);
			return sb.toString();
		}

		@Override
		public int compareTo(final Subfield other) {
			return Integer.compare(this.id, other.id);
		}
		public boolean equals( final Subfield other ) {
			if (other == null) return false;
			if (other.id == this.id) return true;
			return false;
		}

		public Subfield( int id, char code, String value ) {
			this.id = id;
			this.code = code;
			this.value = value;
		}
		public Subfield() {}
	}

	public static class FieldSet implements Comparable<FieldSet> {
		private final Integer id;
		private final String mainTag;
		private final Integer linkNumber;
		private final List<DataField> fields;
		public Integer getId() { return id; }
		public String getMainTag() { return mainTag; }
		public Integer getLinkNumber() { return linkNumber; }
		public List<DataField> getFields() { return fields; }
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append(this.fields.size() + "fields / link number: " +
					this.linkNumber +"/ min field no: " + this.id);
			final Iterator<DataField> i = this.fields.iterator();
			while (i.hasNext()) {
				sb.append(i.next().toString() + "\n");
			}
			return sb.toString();
		}

		@Override
		public int compareTo(final FieldSet other) {
			return Integer.compare(this.id, other.id);
		}
		public boolean equals( final FieldSet other ) {
			if (other == null) return false;
			if (other.id == this.id) return true;
			return false;
		}
		private FieldSet ( Integer id, String mainTag, Integer linkNumber, List<DataField> fields) {
			this.id = id;
			this.mainTag = mainTag;
			this.linkNumber = linkNumber;
			this.fields = fields;
		}
		public static class Builder {
			private Integer id = null;
			private String mainTag = null;
			private Integer linkNumber = null;
			private List<DataField> fields = new ArrayList<>();
			private static final Comparator<DataField> comp;
			static {
				comp = new Comparator<DataField>() {
					@Override
					public int compare(DataField a, DataField b) {
						if (a.tag.equals("880")) {
							if (b.tag.equals("880"))
								return Integer.compare(a.id, b.id);
							return -1;
						}
						if (b.tag.equals("880"))
							return 1;
						return Integer.compare(a.id, b.id);
					}
				};
			}
			public Builder setId(Integer id) {
				this.id = id;
				return this;
			}
			public Builder setMainTag(String mainTag) {
				this.mainTag = mainTag;
				return this;
			}
			public Builder setLinkNumber(Integer linkNumber) {
				this.linkNumber = linkNumber;
				return this;
			}
			public Builder addToFields(DataField field) {
				this.fields.add(field);
				return this;
			}
			public FieldSet build() throws IllegalArgumentException {
				if (id == null)
					throw new IllegalArgumentException("id is a necessary field for a FieldSet.");
				if (mainTag == null)
					throw new IllegalArgumentException("mainTag is a necessary field for a FieldSet.");

				switch (fields.size()) {
				case 0:
					throw new IllegalArgumentException("At least one field is necessary in a FieldSet");
				case 1:
					return new FieldSet(id,mainTag,linkNumber,fields);
				default:
					Collections.sort(fields, comp);
					return new FieldSet(id,mainTag,linkNumber,fields);
				}
			}
		}
	}


	public static enum RecordType {
		BIBLIOGRAPHIC, HOLDINGS, AUTHORITY
	}

	public static enum Script {
		ARABIC, LATIN, CJK, CYRILLIC, GREEK, HEBREW, UNKNOWN
	}

	public static class FieldValues {
		public HeadType type;
		public String author;
		public String title;

		public FieldValues (String author) {
			type = HeadType.AUTHOR;
			this.author = author;
		}
		public FieldValues (String author,String title) {
			if (author != null) {
				type = HeadType.AUTHORTITLE;
				this.author = author;
				this.title = title;
			} else {
				type = HeadType.TITLE;
				this.title = title;
			}
		}
	}
}
