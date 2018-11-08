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

	private HeadingType(final String name,final String abbrev) {
		string = name;
		this.abbrev = abbrev;
	}

	@Override
	public String toString() { return string; }
	public String abbrev() { return abbrev; }

}
