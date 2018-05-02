package edu.cornell.library.integration.marc;

import java.util.Objects;

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

	@Override
    public int hashCode() {
      return Integer.hashCode( this.id );
    }

	@Override
	public boolean equals(final Object o){
		if (this == o) return true;
		if (o == null) return false;
		if (! this.getClass().equals( o.getClass() )) return false;
		Subfield other = (Subfield) o;
		return Objects.equals(this.id, other.id);
	}

	public Subfield( int id, char code, String value ) {
		this.id = id;
		this.code = code;
		this.value = value;
	}
	public Subfield() {}
}
