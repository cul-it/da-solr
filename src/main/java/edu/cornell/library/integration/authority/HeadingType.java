package edu.cornell.library.integration.authority;

import java.util.HashMap;
import java.util.Map;

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

	private static Map<String,HeadingType> _byAuthField = null;
	static {
		_byAuthField = new HashMap<>();
		for (HeadingType ht : HeadingType.values())
			_byAuthField.put(ht.authorityField, ht);
	}
	public static HeadingType byAuthField (String authFieldTag) {
		return _byAuthField.get(authFieldTag);
	}

}
