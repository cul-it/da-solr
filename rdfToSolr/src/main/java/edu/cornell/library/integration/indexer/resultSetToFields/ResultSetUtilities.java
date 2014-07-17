package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
//	public static String RTE = "RTE";
//	public static String PDF = "PDF";
	public static String RTE_openRTL = "\u200E\u202B\u200F";//\u200F - strong RTL invis char
	public static String PDF_closeRTL = "\u200F\u202C\u200E"; //\u200E - strong LTR invis char

	
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
	
	public static String removeTrailingPunctuation ( String s, String unwantedChars ) {
		if (s == null) return null;
		if (unwantedChars == null) return s;
		if (s.equals("")) return s;
		if (unwantedChars.equals("")) return s;
		Pattern p = Pattern.compile ("[" + unwantedChars + "]*("+PDF_closeRTL+"?)*$");
		Matcher m = p.matcher(s);
		return m.replaceAll("$1");
	}
	
	public static String removeAllPunctuation( String s ) {
		if (s == null) return null;
		if (s.equals("")) return s;
		return s.replaceAll("[\\p{Punct}¿¡「」]","");
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
			
		
	/** Gets the var names for a QuerySolution seperated by spaces. */
	protected static String getVarNames( QuerySolution qs){
		Iterator<String> it = qs.varNames();
		String names = "";
		while(it.hasNext()){
			names = names + " " +it.next();
		}
		return names;
	}
}
