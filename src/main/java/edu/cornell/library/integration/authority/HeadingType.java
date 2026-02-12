package edu.cornell.library.integration.authority;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.cornell.library.integration.utilities.StringUtils;

enum HeadingType {
	PERS      ("100"),//0
	CORP      ("110"),
	MEETING   ("111"),
	WORK      ("130"),//3
	EVENT     ("147"),
	ERA       ("148"),
	TOPIC     ("150"),//6
	PLACE     ("151"),
	GENRE     ("155"),
	INSTRUMENT("162"),//9

	SUB_GEN   ("180"),//10
	SUB_GEO   ("181"),
	SUB_ERA   ("182"),
	SUB_GNR   ("185")//13
	;

	private final String authorityField;
	private HeadingType(String authorityField) {
		this.authorityField = authorityField;
	}

	private static Map<String,HeadingType> _byAuthField =
			Stream.of(HeadingType.values()).collect(Collectors.toMap(ht -> ht.authorityField,ht -> ht));

	public static HeadingType byAuthField (String authFieldTag) {
		return _byAuthField.get(authFieldTag);
	}

	public edu.cornell.library.integration.metadata.support.HeadingType getOldHeadingType() {
		switch (this) {
		case PERS:
			return edu.cornell.library.integration.metadata.support.HeadingType.PERSNAME;
		case CORP:
			return edu.cornell.library.integration.metadata.support.HeadingType.CORPNAME;
		case EVENT:
		case MEETING:
			return edu.cornell.library.integration.metadata.support.HeadingType.EVENT;
		case WORK:
			return edu.cornell.library.integration.metadata.support.HeadingType.WORK;
		case ERA:
			return edu.cornell.library.integration.metadata.support.HeadingType.CHRONTERM;
		case TOPIC:
			return edu.cornell.library.integration.metadata.support.HeadingType.TOPIC;
		case PLACE:
			return edu.cornell.library.integration.metadata.support.HeadingType.GEONAME;
		case GENRE:
			return edu.cornell.library.integration.metadata.support.HeadingType.GENRE;
		case INSTRUMENT:
			return edu.cornell.library.integration.metadata.support.HeadingType.MEDIUM;

		default: return null;
		}
	}
}

enum HeadingTypeJsonLD {
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
	private HeadingTypeJsonLD(String madsType) {
		this.madsType = madsType;
	}
	public String getAutorityType() {
		return madsType;
	}

	private static Map<String,HeadingTypeJsonLD> _byAuthType =
			Stream.of(HeadingTypeJsonLD.values()).collect(Collectors.toMap(ht -> ht.madsType,ht -> ht));

	public static HeadingTypeJsonLD byType (String type) {
		return _byAuthType.get(type);
	}
}

