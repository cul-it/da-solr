package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeSpaces;

import java.util.Collection;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;



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

	public static Collection<FieldSet> resultSetsToSetsofMarcFields( Map<String, ResultSet> results ) {
		return resultSetsToSetsofMarcFields(results,null);
	}
	public static Collection<FieldSet> resultSetsToSetsofMarcFields(
			Map<String, ResultSet> results, Map<String,String> q2f ) {
		MarcRecord rec = new MarcRecord();
		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			if (q2f != null && q2f.containsKey(resultKey))
				rec.addDataFieldResultSet(rs,q2f.get(resultKey));
			else
				rec.addDataFieldResultSet(rs);
		}
		Collection<FieldSet> sortedFields = rec.matchAndSortDataFields();

		return sortedFields;
	}
}
