package edu.cornell.library.integration.metadata.support;

import java.util.Objects;

public class Heading implements Comparable<Heading> {
	Integer id = null;
	final String displayForm;
	final String sort;
	final HeadingType headingType;
	Heading parent = null;

	public Heading( Integer id, String heading, String headingSort, HeadingType headingType ) {
		this.id = id;
		this.displayForm = heading;
		this.sort = headingSort;
		this.headingType = headingType;
	}
	public Heading( String heading, String headingSort, HeadingType headingType ) {
		this.displayForm = heading;
		this.sort = headingSort;
		this.headingType = headingType;
	}
	public Heading( String heading, String headingSort, HeadingType HeadingType, Heading parent ) {
		this(heading,headingSort,HeadingType);
		this.parent = parent;
	}

	public String sort()             { return this.sort; }
	public Heading parent()          { return this.parent; }
	public Integer id()              { return this.id; }
	public String displayForm()      { return this.displayForm; }
	public HeadingType headingType() { return this.headingType; }

	public void setId( Integer id )  { this.id = id; }

	@Override
	public String toString () {
		return String.format("[heading_type: %s; sort: \"%s\"%s]",
				this.headingType.toString(), this.displayForm,
				(this.parent != null)?String.format(" parent:[%s]",this.parent.toString()):"");
	}
	@Override
	public int hashCode() { return Integer.hashCode( this.id ); }
	@Override
	public int compareTo(final Heading other) { return Integer.compare(this.id, other.id);	}
	@Override
	public boolean equals(final Object o) {
		if (this == o) return true;
		if (o == null) return false;
		if (! this.getClass().equals( o.getClass() )) return false;
		Heading other = (Heading) o;
		return Objects.equals(this.id, other.id);
	}
}
