package edu.cornell.library.integration.authority.mads;

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

	@Override
	public String toString() { return this.name; }

	public String prefix() { return this.idPrefix; }
}

