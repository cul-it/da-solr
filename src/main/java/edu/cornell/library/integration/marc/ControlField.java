package edu.cornell.library.integration.marc;

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
		public boolean equals( final ControlField other ) {
			if (other == null) return false;
			if (other.id == this.id) return true;
			return false;
		}
	}
