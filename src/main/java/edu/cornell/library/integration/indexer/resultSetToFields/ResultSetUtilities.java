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
		else if ( node.canAs( Literal.class )){			
			return heyItsUtf8( ((Literal)node).getLexicalForm() ) ;
						
		}else {
			return heyItsUtf8( node.toString() );			
		}
	}	
	
	/**
	 * not sure what the hell is going on here.
	 * Results from vitruoso are in iso-8859-1?
	 * Some how this worked in Sept 2012 and even
	 * got Chinese characters. 
	 */
	private static String heyItsUtf8(String s){
		return s;
		
		//do something like this if using virtuoso?
		//Maybe next time try to set some HTTP header to request utf8?
//		try {
//			return new String(s.getBytes("iso-8859-1"), "utf-8") ;
//		} catch (UnsupportedEncodingException e) {
//			throw new Error("all java implementations are required to implement utf-8");
//		}		 		
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
	
	/** 
	 * This method will take a key and a ResultSet and will find
	 * the row where the column named 'key' matches the 
	 * value of the key argument.
	 * 
	 * If this row is found, the value from the column 'value' is returned.
	 * Otherwise null is returned.
	 * 
	 * Null is returned if the key is not found or if rs or key are null.
	 * @throws Exception if no key column found this throws an exception.
	 */
	public static String findValueByKey( ResultSet rs, String key) throws Exception{
		QuerySolution qs = findRow( rs, key );		
		if( qs == null ) return null;
				
		if( qs.getLiteral("value") != null ){
			return qs.getLiteral("value").getLexicalForm();
		} else {
			Resource res = qs.getResource("value");
			if( res != null ){
				throw new Exception("findValueByKey() requires a column named 'value', " +
						"but none was found in columns: " + getVarNames(qs));
 			}else{
				return null;
			}
		}
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