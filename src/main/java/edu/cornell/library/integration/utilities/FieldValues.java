package edu.cornell.library.integration.utilities;

import java.util.ArrayList;
import java.util.List;

import edu.cornell.library.integration.indexer.utilities.BrowseUtils.HeadType;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.Subfield;

public class FieldValues {
	public HeadType type;
	public String author;
	public String title;

	public FieldValues (String author) {
		this.type = HeadType.AUTHOR;
		this.author = author;
	}
	public FieldValues (String author,String title) {
		if (author != null) {
			this.type = HeadType.AUTHORTITLE;
			this.author = author;
		} else {
			this.type = HeadType.TITLE;
		}
		this.title = title;
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.type.toString()).append(": ");
		if (this.author != null) {
			sb.append("A(").append(this.author).append(')');
			if (this.title != null)
				sb.append(" | T(").append(this.title).append(')');
		} else if (this.title != null)
			sb.append("T(").append(this.title).append(')');
		return sb.toString();
	}

	/**
	 * The field may be an a name, a name and title or just a title depending on the
	 * field type and contents. Extract the value(s).
	 * @return FieldValues
	 */
	public static FieldValues getFieldValuesForNameAndOrTitleField(DataField f, String subfields) {
		char one = f.mainTag.charAt(0), two = f.mainTag.charAt(1), three = f.mainTag.charAt(2);
		if ((three=='0' && (two=='0' || two=='1' || two=='2'))
				|| (three=='1' && two=='1'))
			return getFieldValuesForNameMaybeTitleField_x00_x10_x11( f, subfields );
		if (one=='7'&&(two=='6'||two=='7'||two=='8'))
			return getFieldValuesForTitleMaybeName_76x_77x_78x( f, subfields );
		if (three == '0' && (two == '3' || two == '4' || two == '9'))
			return getFieldValuesForTitleOnly_x30_x40_490( f, subfields );
		throw new IllegalArgumentException( "Method DataField.getFieldValuesForNameAndOrTitleField() "
				+ "called for unsupported field ("+f.mainTag+").");
	}

	/* These fields are exclusively title fields, so only a title is expected */
	private static FieldValues getFieldValuesForTitleOnly_x30_x40_490(DataField f, String subfields) {
		return new FieldValues( null, f.concatenateSpecificSubfields(subfields) );
	}

	/* These fields are primarily title fields, so a title is expected, while name data may
	 * optionally be present.
	 */
	private static FieldValues getFieldValuesForTitleMaybeName_76x_77x_78x(DataField f, String subfields) {
		boolean hasA = false;
		boolean hasDefiniteTitleSubfield = false;
		MeaningOfSubfieldABasedOnSubfield7 seven = MeaningOfSubfieldABasedOnSubfield7.UNK;
		subfields = subfields.replaceAll("c", "")+"d";

		for (Subfield sf : f.subfields)
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
			return getFieldValuesForTitleMaybeName_76x_77x_78x_variantAuthorTitle(f, subfields);
		return new FieldValues(null,f.concatenateSpecificSubfields(subfields));
	}
	/* This particular linking field entry seems to contain both author and title data. */
	private static FieldValues getFieldValuesForTitleMaybeName_76x_77x_78x_variantAuthorTitle(
			DataField f, String subfields) {
		List<String> authorSubfields = new ArrayList<>();
		List<String> titleSubfields = new ArrayList<>();
		boolean foundTitle = false;
		for(Subfield sf : f.subfields) {
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
	private static FieldValues getFieldValuesForNameMaybeTitleField_x00_x10_x11(DataField f, String subfields) {
		List<String> authorSubfields = new ArrayList<>();
		List<String> titleSubfields = new ArrayList<>();
		boolean foundTitle = false;
		for(Subfield sf : f.subfields) {
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

}
