package edu.cornell.library.integration.marc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class DataFieldSet implements Comparable<DataFieldSet> {
	private final Integer id;
	private final String mainTag;
	private final Integer linkNumber;
	private final List<DataField> fields;
	public Integer getId() { return id; }
	public String getMainTag() { return mainTag; }
	public Integer getLinkNumber() { return linkNumber; }
	public List<DataField> getFields() { return fields; }
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(this.fields.size() + "fields / link number: " +
				this.linkNumber +"/ min field no: " + this.id);
		final Iterator<DataField> i = this.fields.iterator();
		while (i.hasNext()) {
			sb.append(i.next().toString() + "\n");
		}
		return sb.toString();
	}

	@Override
	public int compareTo(final DataFieldSet other) {
		return Integer.compare(this.id, other.id);
	}
	public boolean equals( final DataFieldSet other ) {
		if (other == null) return false;
		if (other.id == this.id) return true;
		return false;
	}
	private DataFieldSet ( Integer id, String mainTag, Integer linkNumber, List<DataField> fields) {
		this.id = id;
		this.mainTag = mainTag;
		this.linkNumber = linkNumber;
		this.fields = fields;
	}
	public static class Builder {
		private Integer id = null;
		private String mainTag = null;
		private Integer linkNumber = null;
		private List<DataField> fields = new ArrayList<>();
		private static final Comparator<DataField> comp;
		static {
			comp = new Comparator<DataField>() {
				@Override
				public int compare(DataField a, DataField b) {
					if (a.tag.equals("880")) {
						if (b.tag.equals("880"))
							return Integer.compare(a.id, b.id);
						return -1;
					}
					if (b.tag.equals("880"))
						return 1;
					return Integer.compare(a.id, b.id);
				}
			};
		}
		public Builder setId(Integer id) {
			this.id = id;
			return this;
		}
		public Integer getId() {
			return this.id;
		}
		public Builder setMainTag(String mainTag) {
			this.mainTag = mainTag;
			return this;
		}
		public String getMainTag() {
			return this.mainTag;
		}
		public Builder setLinkNumber(Integer linkNumber) {
			this.linkNumber = linkNumber;
			return this;
		}
		public Integer getLinkNumber() {
			return this.linkNumber;
		}
		public Builder addToFields(DataField field) {
			this.fields.add(field);
			return this;
		}
		public Builder addToFields(List<DataField> fields) {
			this.fields.addAll(fields);
			return this;
		}
		public DataFieldSet build() throws IllegalArgumentException {
			if (id == null)
				throw new IllegalArgumentException("id is a necessary field for a FieldSet.");
			if (mainTag == null)
				throw new IllegalArgumentException("mainTag is a necessary field for a FieldSet.");

			switch (fields.size()) {
			case 0:
				throw new IllegalArgumentException("At least one field is necessary in a FieldSet");
			case 1:
				return new DataFieldSet(id,mainTag,linkNumber,fields);
			default:
				Collections.sort(fields, comp);
				return new DataFieldSet(id,mainTag,linkNumber,fields);
			}
		}
	}
}

