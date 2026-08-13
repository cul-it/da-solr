package edu.cornell.library.integration.authority.mads;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum MadsHeadingType {
	UNKNOWN("unknown"),
	COMPLEX_SUBJECT("madsrdf:ComplexSubject"),
	CONFERENCE_NAME("madsrdf:ConferenceName"),
	CORPORATE_NAME("madsrdf:CorporateName"),
	FAMILY_NAME("madsrdf:FamilyName"),
	GENRE_FORM("madsrdf:GenreForm"),
	GEOGRAPHIC("madsrdf:Geographic"),
	HIERARCHICAL_GEOGRAPHIC("madsrdf:HierarchicalGeographic"),
	LANGUAGE("madsrdf:Language"),
	NAME_TITLE("madsrdf:NameTitle"),
	OCCUPATION("madsrdf:Occupation"),
	PERSONAL_NAME("madsrdf:PersonalName"),
	TEMPORAL("madsrdf:Temporal"),
	TITLE("madsrdf:Title"),
	TOPIC("madsrdf:Topic"),
	MEDIUM("madsrdf:Medium")
	;

	private final String madsType;
	private MadsHeadingType(String madsType) {
		this.madsType = madsType;
	}

	public String getAutorityType() {
		return madsType;
	}

	private static final Map<Integer,MadsHeadingType> _byOrdinal =
			Stream.of(MadsHeadingType.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static MadsHeadingType byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}

	private static final Map<String,MadsHeadingType> _byAuthType =
			Stream.of(MadsHeadingType.values()).collect(Collectors.toMap(ht -> ht.madsType,ht -> ht));

	public static MadsHeadingType byType (String type) {
		return _byAuthType.get(type);
	}
}
