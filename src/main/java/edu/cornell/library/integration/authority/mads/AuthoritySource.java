package edu.cornell.library.integration.authority.mads;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum AuthoritySource {
	/*
	 * Enumeration of authority sources.
	 */
	LC    ("Library of Congress",                           "c"),
	LCNAF ("Library of Congress Name Authority File",       "n"),
	LCSH  ("Library of Congress Subject Headings",          "s"),
	LCGFT ("Library of Congress Genre/Form Terms",          "g"),
	LCJSH ("Library of Congress Juvenile Subject Headings", "j"),
	FAST  ("Faceted Application of Subject Terminology",    "f"),
	OTHER ("Other Authority Source",                        "o"),
	UNK   ("Unknown Authority Source",                      "u"),
	LOCAL ("Local Authority File",                          "loc");

	private final String name;
	private final String idPrefix;

	private AuthoritySource(final String name, final String idPrefix) {
		this.name = name;
		this.idPrefix = idPrefix;
	}

	private static final Map<Integer,AuthoritySource> _byOrdinal =
			Stream.of(AuthoritySource.values()).collect(Collectors.toMap(s -> s.ordinal(), s -> s));

	public static AuthoritySource byOrdinal( int ordinal ) {
		return _byOrdinal.get(ordinal);
	}

	@Override
	public String toString() { return this.name; }

	public String prefix() { return this.idPrefix; }
}

