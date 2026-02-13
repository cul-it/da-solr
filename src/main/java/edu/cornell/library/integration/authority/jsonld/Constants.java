package edu.cornell.library.integration.authority.jsonld;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Constants {
	public static final String SUBDIV_URL = "http://id.loc.gov/authorities/subjects/collection_Subdivisions";
	public static final String UNDIFF_URL = "http://id.loc.gov/authorities/names/collection_NamesUndifferentiated";

	private Constants() {}
}

enum MadsHeadingType {
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
	TOPIC("madsrdf:Topic")
	;

	private final String madsType;
	private MadsHeadingType(String madsType) {
		this.madsType = madsType;
	}
	public String getAutorityType() {
		return madsType;
	}

	private static Map<String,MadsHeadingType> _byAuthType =
			Stream.of(MadsHeadingType.values()).collect(Collectors.toMap(ht -> ht.madsType,ht -> ht));

	public static MadsHeadingType byType (String type) {
		return _byAuthType.get(type);
	}
}

enum MadsRecordStatus {
	NEW("new"), REVISED("revised"), DEPRECATED("deprecated");

	private final String display;
	private MadsRecordStatus( String display ) {
		this.display = display;
	}
	public String getDisplay() {
		return this.display;
	}

	private static Map<Integer,MadsRecordStatus> _byOrdinal =
			Stream.of(MadsRecordStatus.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static MadsRecordStatus byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}

	public static MadsRecordStatus byName(String recordStatus) {
		for (MadsRecordStatus status : MadsRecordStatus.values()) {
            if (status.display.equalsIgnoreCase(recordStatus) || status.name().equalsIgnoreCase(recordStatus)) {
                return status;
            }
        }
        throw new IllegalArgumentException("No constant with valid record status " + recordStatus + " found");
	}
}
