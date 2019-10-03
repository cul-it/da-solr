package edu.cornell.library.integration.metadata.support;

public enum HeadingCategory {
	AUTHOR("author","works_by"),
	SUBJECT("subject","works_about"),
	AUTHORTITLE("authortitle","works"),
	TITLE("title","works");

	private final String string;
	private final String field;

	private HeadingCategory(final String name, final String dbField) {
		this.string = name;
		this.field = dbField;
	}

	@Override
	public String toString() { return this.string; }
	public String dbField() { return this.field; }

}
