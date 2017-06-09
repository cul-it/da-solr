package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeSpaces;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;



/**
 * static utility methods for working with ResultSet objects.
 * These are intended as publicly usable code.
 */
public class ResultSetUtilities {

	public static String nodeToString(RDFNode node){
		if( node == null )
			return "";
		if ( node.canAs( Literal.class ))
			return ((Literal)node).getLexicalForm();
		return node.toString();
	}

	public static void addField( Map<String, SolrInputField> fields, String fieldName, String value ) {
		addField(fields, fieldName, value, false);
	}

	public static void addField( Map<String, SolrInputField> fields,
			String fieldName, String value, Boolean dedupeValue ) {
		if ((value == null) || (value.equals(""))) return;
		value = value.trim();
		if (value.equals("")) return;
		if (fieldName.endsWith("_t")) // DISCOVERYACCESS-1408
			value = standardizeApostrophes(value);
		else if (fieldName.endsWith("_facet")) // DISCOVERYACCESS-3061
			value = standardizeSpaces(value);
		SolrInputField field = fields.get(fieldName);
		if( field == null ){
			field = new SolrInputField(fieldName);
			fields.put(fieldName,field);
		}
		if ( ! dedupeValue || field.getValueCount() == 0 || ! field.getValues().contains(value) )
			field.addValue(value,1.0f);
	}

	public static void addField( Map<String, SolrInputField> fields, String fieldName, Boolean value ) {
		final SolrInputField field = new SolrInputField(fieldName);
		field.setValue(value, 1.0f);
		fields.put(fieldName, field);
	}

	public static class SolrField {
		String fieldName;
		String fieldValue;
		public SolrField ( String fieldName, String fieldValue ) {
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}
	}
	public static class BooleanSolrField {
		String fieldName;
		Boolean fieldValue;
		public BooleanSolrField ( String fieldName, Boolean fieldValue ) {
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}
	}
	public static class SolrFields {
		List<SolrField> fields = new ArrayList<>();
		List<BooleanSolrField> boolFields = new ArrayList<>();
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
		public String toString() {
			StringBuilder sb = new StringBuilder();
			for (SolrField f : this.fields)
				sb.append(f.fieldName).append(": ").append(f.fieldValue).append('\n');
			for (BooleanSolrField f : this.boolFields)
				sb.append(f.fieldName).append(": ").append(f.fieldValue).append('\n');
			return sb.toString();
		}
	}
}
