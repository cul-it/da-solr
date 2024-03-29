package edu.cornell.library.integration.metadata.support;

public enum HeadingType {

	PERSNAME("Personal Name","pers"),
	CORPNAME("Corporate Name","corp"),
	EVENT("Event","event"),
	GENHEAD("General Heading","gen"),
	TOPIC("Topical Term","topic"),
	GEONAME("Geographic Name","geo"),
	CHRONTERM("Chronological Term","era"),
	GENRE("Genre/Form Term","genr"),
	MEDIUM("Medium of Performance","med"),
	WORK("Work","work");

	private final String string;
	private final String abbrev;
	private String ordering = null;

	private HeadingType(final String name,final String abbrev) {
		this.string = name;
		this.abbrev = abbrev;
	}
	private HeadingType(final String name,final String abbrev,String ordering) {
		this.string = name;
		this.abbrev = abbrev;
		this.ordering = ordering;
	}

	@Override
	public String toString() { return this.string; }
	public String abbrev() { return this.abbrev; }

	public static HeadingType byOrdering(String ordering) {
		if (ordering == null) return null;
		switch (ordering) {
		case "1": return HeadingType.PERSNAME;
		case "2": return HeadingType.CORPNAME;
		default: return HeadingType.EVENT;
		}
	}

}
