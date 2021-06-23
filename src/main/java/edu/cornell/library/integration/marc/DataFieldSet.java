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
	public Integer getId() { return this.id; }
	public String getMainTag() { return this.mainTag; }
	public Integer getLinkNumber() { return this.linkNumber; }
	public List<DataField> getFields() { return this.fields; }
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

	@Override
	public boolean equals( final Object o ) {
		if (o == null) return false;
		if (! this.getClass().equals(o.getClass())) return false;
		return this.id.equals( ((DataFieldSet)o).id );
	}

	@Override
	public int hashCode() {
		return this.id.hashCode();
	}

	DataFieldSet ( Integer id, String mainTag, Integer linkNumber, List<DataField> fields) {
		this.id = id;
		this.mainTag = mainTag;
		this.linkNumber = linkNumber;
		this.fields = fields;
	}
	static class Builder {
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
		Builder setId(Integer id) {
			this.id = id;
			return this;
		}
		public Integer getId() {
			return this.id;
		}
		Builder setMainTag(String mainTag) {
			this.mainTag = mainTag;
			return this;
		}
		public String getMainTag() {
			return this.mainTag;
		}
		Builder setLinkNumber(Integer linkNumber) {
			this.linkNumber = linkNumber;
			return this;
		}
		public Integer getLinkNumber() {
			return this.linkNumber;
		}
		Builder addToFields(DataField field) {
			this.fields.add(field);
			return this;
		}
		Builder addToFields(List<DataField> addedFields) {
			this.fields.addAll(addedFields);
			return this;
		}
		DataFieldSet build() throws IllegalArgumentException {
			if (this.id == null)
				throw new IllegalArgumentException("id is a necessary field for a FieldSet.");
			if (this.mainTag == null)
				throw new IllegalArgumentException("mainTag is a necessary field for a FieldSet.");

			switch (this.fields.size()) {
			case 0:
				throw new IllegalArgumentException("At least one field is necessary in a FieldSet");
			case 1:
				return new DataFieldSet(this.id,this.mainTag,this.linkNumber,this.fields);
			default:
				Collections.sort(this.fields, comp);
				return new DataFieldSet(this.id,this.mainTag,this.linkNumber,this.fields);
			}
		}
	}
}

