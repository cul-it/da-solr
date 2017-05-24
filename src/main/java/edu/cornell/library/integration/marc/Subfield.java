package edu.cornell.library.integration.marc;

public class Subfield implements Comparable<Subfield> {

	public int id;
	public Character code;
	public String value;

	@Override
	public String toString() {
		return this.toString('\u2021');
	}

	public String toString(final Character subFieldSeparator) {
		final StringBuilder sb = new StringBuilder();
		sb.append(subFieldSeparator);
		sb.append(this.code);
		sb.append(" ");
		sb.append(this.value);
		return sb.toString();
	}

	@Override
	public int compareTo(final Subfield other) {
		return Integer.compare(this.id, other.id);
	}
	public boolean equals( final Subfield other ) {
		if (other == null) return false;
		if (other.id == this.id) return true;
		return false;
	}

	public Subfield( int id, char code, String value ) {
		this.id = id;
		this.code = code;
		this.value = value;
	}
	public Subfield() {}
}
