package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeSpaces;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.DataFieldSet;



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

	public static Collection<DataFieldSet> resultSetsToSetsofMarcFields( Map<String, ResultSet> results ) {
		MarcRecord rec = new MarcRecord();
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			rec.addDataFieldResultSet(rs);
		}
		return rec.matchAndSortDataFields();
	}

	public static class SolrField {
		String fieldName;
		String fieldValue;
		public SolrField ( String fieldName, String fieldValue ) {
			this.fieldName = fieldName;
			this.fieldValue = fieldValue;
		}
		public boolean equals( final SolrField other ){
			if (other == null)
				return false;
			if (this.fieldName == null) {
				if (other.fieldName != null) return false;
			} else {
				if ( ! this.fieldName.equals(other.fieldName) ) return false;
			}
			if (this.fieldValue == null) {
				if (other.fieldValue != null) return false;
			} else {
				if ( ! this.fieldValue.equals(other.fieldValue) ) return false;
			}
			return true;
		}
	}
	public static class SolrFields {
		List<SolrField> fields = new ArrayList<>();
		public void addAll(List<SolrField> sfs) {
			if (sfs == null) return;
			this.fields.addAll(sfs);
		}
		public boolean equals( final SolrFields other) {
			if (other == null)
				return false;
			if (this.fields.size() != other.fields.size())
				return false;
			for (int i = 0; i < this.fields.size(); i++)
				if ( ! this.fields.get(i).equals(other.fields.get(i)))
					return false;
			return true;
		}
	}
}
