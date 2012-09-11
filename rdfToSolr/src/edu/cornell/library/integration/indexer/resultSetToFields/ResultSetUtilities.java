package edu.cornell.library.integration.indexer.resultSetToFields;

import java.util.Iterator;

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
			}else if( res.getURI()!= null){
				return res.getURI();
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
