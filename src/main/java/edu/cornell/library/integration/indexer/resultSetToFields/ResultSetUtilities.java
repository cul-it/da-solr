package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;



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

	public static void addField( Map<String, SolrInputField> fields, String fieldName, String value) {
		if ((value == null) || (value.equals(""))) return;
		value = value.trim();
		if (value.equals("")) return;
		SolrInputField field = fields.get(fieldName);
		if( field == null ){
			field = new SolrInputField(fieldName);
			fields.put(fieldName,field);
		}
		field.addValue(value,1.0f);
	}

	/** 
	 * This method will take a key and a ResultSet and will return
	 * the QuerySolution where the column value named 'key' matches the
	 * value of the key argument.
	 * 
	 * Null is returned if the key is not found or if rs or key are null.
	 * @throws Exception if no key column found this throws an exception.
	 */
	public static QuerySolution findRow ( ResultSet rs, String key) throws Exception{
		if( rs==null || key == null) return null;

		while(rs.hasNext()){
			QuerySolution qs = rs.nextSolution();
			RDFNode node = qs.get("key");

			if( node == null ){
				throw new Exception("findRow() requires a column named 'key', " +
						"but none was found in columns: " + getVarNames(qs));
			}

			if( node.isLiteral() ){
				if( key.equals( ((Literal)node).getLexicalForm() ) ){
					return qs;
				}
			}else if( node.isURIResource() ){
				if( key.equals( ((Resource)node).getURI() )){
					return qs;
				}
			}
		}
		//nothing found,
		return null;
	}

	/** Gets the var names for a QuerySolution separated by spaces. */
	protected static String getVarNames( QuerySolution qs){
		Iterator<String> it = qs.varNames();
		String names = "";
		while(it.hasNext()){
			names = names + " " +it.next();
		}
		return names;
	}
}
