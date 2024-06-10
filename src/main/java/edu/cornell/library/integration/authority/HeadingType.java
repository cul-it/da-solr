package edu.cornell.library.integration.authority;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
