package edu.cornell.library.integration.marc;

import java.util.Objects;

public class ControlField implements Comparable<ControlField> {

		public int id;
		public String tag;
		public String value;

		public ControlField( int id, String tag, String value ) {
			this.id = id;
			this.tag = tag;
			this.value = value;
		}
		@Override
		public int compareTo(final ControlField other) {
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
			ControlField other = (ControlField) o;
			return Objects.equals(this.id, other.id);
		}

		@Override
		public String toString() {
			return this.tag+" "+this.value;
		}
	}
