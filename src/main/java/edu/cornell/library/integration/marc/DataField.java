package edu.cornell.library.integration.marc;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class DataField implements Comparable<DataField> {

	// Characters to frame Right-to-left text blocks to support display in mixed directional text blocks
	public static String RLE_openRTL = "\u200E\u202B\u200F";//\u200F - strong RTL invis char
	public static String PDF_closeRTL = "\u200F\u202C\u200E"; //\u200E - strong LTR invis char

	public int id;
	public String tag;
//	public String alttag; //subfield 6 tag number for an 880 field
	public Character ind1 = ' ';
	public Character ind2 = ' ';
	public TreeSet<Subfield> subfields = new TreeSet<>();

	public Integer linkNumber; //from MARC subfield 6
	public String mainTag = null;

	@Override
	public String toString() {
		return this.toString('\u2021');
	}

	String toString(final Character subfieldSeparator) {
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

	StringBuilder toStringBuilder() {
		return this.toStringBuilder('\u2021');
	}

	private StringBuilder toStringBuilder(final Character subfieldSeparator) {
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
		return sb;
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
		case "440":
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
						return RLE_openRTL + stripLeadCharsFromString(
								fulltitle.substring(RLE_openRTL.length()),nonFilingCharCount, titlePrefixChars);
					return stripLeadCharsFromString(fulltitle, nonFilingCharCount, titlePrefixChars);
				} catch (IllegalArgumentException e) {
					System.out.println("Initial article not stripped from title: "+
							e.getMessage()+" "+nonFilingCharCount+"/"+fulltitle);
				}
		}
		return fulltitle;
	}
	private final static String titlePrefixChars = "[.\"“";

	/* Parse a series of subfields in a single string into a set of Subfield objects */
	private static TreeSet<Subfield> parseSubfields(String subfields, Character subfieldSeparator) {
		String[] values = subfields.split(String.valueOf(subfieldSeparator));
		TreeSet<Subfield> subfieldSet = new TreeSet<>();
		for (int i = 1 ; i < values.length ; i++ ) {
			String val = values[i];
			if (val.isEmpty())
				continue;
			if (val.length() == 1)
				subfieldSet.add( new Subfield( i, val.charAt(0), "") );
			else
				subfieldSet.add( new Subfield( i, val.charAt(0), val.substring(1).trim()) );
		}
		return subfieldSet;
	}

	@Override
	public int compareTo(final DataField other) {
		return Integer.compare(this.id, other.id);
	}


	@Override
    public int hashCode() {
      return Integer.hashCode( this.id );
    }

    @Override
	public boolean equals(final Object o){
		if (this == o) return true;
		if (o == null) return false;
		if (! this.getClass().equals( o.getClass() )) return false;
		DataField other = (DataField) o;
		return Objects.equals(this.id, other.id);
	}

	/**
	 * Remove the number of characters from the beginning of <b>s</b> to constitute the first
	 * <b>targetCount</b> characters <b>s</b>, counting combining diacritics as separate characters.
	 * Any characters found in <b>reserves</b> will not be removed, but will still count toward the
	 * <b>targetCount</b> characters.<br/><br/>
	 * If character number <b>targetCount</b> is not the last byte of its containing grapheme, the entire
	 * partial character will be removed.<br/><br/>
	 * The implementation will return the shortened string in Unicode NFC form, even if it was not supplied
	 * that way.
	 * @param s
	 *  Original string
	 * @param targetCount
	 *  Number of characters (counting combining diacritics separately) to remove from the beginning of string
	 * @param reserves
	 *  Characters that should not be removed, but will still count toward the <b>targetCount</b> characters.
	 * @return
	 *  Stripped string
	 * @throws IllegalArgumentException
	 *  will be thrown if <b>targetCount</b> characters exceeds the length of string <b>s</b>.
	 */
	static String stripLeadCharsFromString( String s, Integer targetCount, String reserves )
			throws IllegalArgumentException {

		int pos = 0;
		int count = 0;
		StringBuilder foundReserves = new StringBuilder();
		s = Normalizer.normalize(s, Normalizer.Form.NFD);
		if ( s.length() < targetCount )
			throw new IllegalArgumentException(
					"Requested bytes to strip is longer than string in UTF-8");
		while (count < targetCount) {
			char c = s.charAt(pos);

			if (Character.isHighSurrogate(c))
				pos++;
			pos++;
			count++;
			if (reserves.indexOf(c) != -1)
				foundReserves.append(c);
		}
		String substr = s.substring(pos);
		while (substr.length() > 0 &&
				Character.getType(substr.charAt(0)) == Character.NON_SPACING_MARK)
			substr = substr.substring(1);
		return foundReserves.toString()
				+ Normalizer.normalize(substr,Normalizer.Form.NFC);

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
	public DataField( int id, String tag, Character ind1, Character ind2, TreeSet<Subfield> subfields ) {
		this.id = id;
		this.tag = tag;
		this.ind1 = ind1;
		this.ind2 = ind2;
		this.mainTag = tag;
		this.subfields = subfields;
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

	public static enum Script {
		ARABIC, LATIN, CJK, CYRILLIC, GREEK, HEBREW, UNKNOWN
	}
}

