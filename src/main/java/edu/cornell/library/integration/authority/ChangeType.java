package edu.cornell.library.integration.authority;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum ChangeType {
	NEW("New"), UPDATE("Update"), DELETE("Delete");

	private final String display;
	private ChangeType( String display ) {
		this.display = display;
	}
	public String getDisplay() {
		return this.display;
	}

	private static Map<Integer,ChangeType> _byOrdinal =
			Stream.of(ChangeType.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static ChangeType byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}
}
