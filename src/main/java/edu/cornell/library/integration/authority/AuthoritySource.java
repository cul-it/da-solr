package edu.cornell.library.integration.authority;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum AuthoritySource {
	LC, NAF, LCSH, LCGFT, LCJSH, FAST, OTHER, UNK;

	private static Map<Integer,AuthoritySource> _byOrdinal =
			Stream.of(AuthoritySource.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static AuthoritySource byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}


}
