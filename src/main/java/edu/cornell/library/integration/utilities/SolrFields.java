package edu.cornell.library.integration.utilities;

import java.util.ArrayList;
import java.util.List;

public class SolrFields {

	public List<SolrField> fields = new ArrayList<>();
	public List<BooleanSolrField> boolFields = new ArrayList<>();
	Integer recordBoost = 1;
	public Integer getRecordBoost() {
		return this.recordBoost;
	}
	public void setRecordBoost(Integer recordBoost) {
		this.recordBoost = recordBoost;
	}
	public void add( String field, String value ) {
		if (field == null || field.isEmpty() || value == null || value.isEmpty()) return;
		this.fields.add(new SolrField(field,value));
	}
	public void add( String field, Boolean value ) {
		if (field == null || field.isEmpty() || value == null ) return;
		this.boolFields.add(new BooleanSolrField(field,value));
	}
	public void add( SolrField sf ) {
		if (sf == null) return;
		this.fields.add(sf);
	}
	public void add( BooleanSolrField sf ) {
		if (sf == null) return;
		this.boolFields.add(sf);
	}
	public void addAll(List<SolrField> sfs) {
		if (sfs == null) return;
		this.fields.addAll(sfs);
	}
	public void addAll( SolrFields other ) {
		this.fields.addAll( other.fields );
		this.boolFields.addAll( other.boolFields );
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (this.recordBoost != 1)
			sb.append('^').append(this.recordBoost).append('\n');
		for (SolrField f : this.fields)
			sb.append(f.fieldName).append(": ").append(f.fieldValue).append('\n');
		for (BooleanSolrField f : this.boolFields)
			sb.append(f.fieldName).append(": ").append(f.fieldValue).append('\n');
		return sb.toString();
	}

	public static class SolrField {
		public String fieldName;
		public String fieldValue;
		public SolrField ( String fieldName, String fieldValue ) {
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}
	}
	public static class BooleanSolrField {
		public String fieldName;
		public Boolean fieldValue;
		public BooleanSolrField ( String fieldName, Boolean fieldValue ) {
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}
	}

}
