package edu.cornell.library.integration.marc;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.RLE_openRTL;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.utilities.CharacterSetUtils;

public class DataField implements Comparable<DataField> {

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
		final String titlePrefixChars = "[.\"“";

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
	public DataField( int id, int linkNumber, String tag, Character ind1, Character ind2, String subfields, Boolean is880 ) {
		this.id = id;
		this.linkNumber = linkNumber;
		this.tag = (is880)?"880":tag;
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

	public static class FieldValues {
		public HeadType type;
		public String author;
		public String title;

		public FieldValues (String author) {
			type = HeadType.AUTHOR;
			this.author = author;
		}
		public FieldValues (String author,String title) {
			type = HeadType.AUTHORTITLE;
			this.author = author;
			this.title = title;
		}
	}
	public static enum Script {
		ARABIC, LATIN, CJK, CYRILLIC, GREEK, HEBREW, UNKNOWN
	}
}

