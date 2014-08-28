package edu.cornell.library.integration.indexer.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class IndexingUtilities {
	
	public static void optimizeIndex( String solrCoreURL ) {
		System.out.println("Optimizing index at: "+solrCoreURL+". This may take a while...");
		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("\tstarting at: "+dateFormat.format(Calendar.getInstance().getTime()));
		try {
			URL queryUrl = new URL(solrCoreURL + "/update?optimize=true");
			InputStream in = queryUrl.openStream();
			BufferedReader buff = new BufferedReader(new InputStreamReader(in));
			String line;
			while ( (line = buff.readLine()) != null ) 
				System.out.println(line);
			buff.close();
			in.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.exit(1); 
		} catch (IOException e) {
			// With the Apache timeout set sufficiently high, an IOException should represent an actual problem.
			e.printStackTrace();
			System.exit(1);
		}
		System.out.println("\tcompleted at: "+dateFormat.format(Calendar.getInstance().getTime()));
	}
	
	public static String substituteInRecordURI(String recordURI, String query) {
		if( query == null )
			return null;			
		return query.replaceAll("\\$recordURI\\$", "<"+recordURI+">");		
	}

	public static String toString(SolrInputDocument doc) {
		String out ="SolrInputDocument[\n" ;
		for( String name : doc.getFieldNames()){
			SolrInputField f = doc.getField(name);
			out = out + "  " + name +": '" + f.toString() + "'\n";
		}
		return out + "]\n";						
	}
	
	/**
	 * For all newFields, add them to doc, taking into account
	 * fields that already exist in doc and merging the
	 * values of the new and existing fields. 
	 */
	public static void combineFields(SolrInputDocument doc,
			Map<? extends String, ? extends SolrInputField> newFields) {		
		for( String newFieldKey : newFields.keySet() ){
			SolrInputField newField = newFields.get(newFieldKey);

			if (newField.getValueCount() == 0) continue;
			
			if( doc.containsKey( newFieldKey )){
				SolrInputField existingField=doc.get(newFieldKey);
				mergeValuesForField(existingField, newField);
			}else{
				doc.put(newFieldKey, newField);				
			}
		}		
	}

	/**
	 * Call existingField.addValue() for all values form newField.
	 */
	public static void mergeValuesForField(SolrInputField existingField,
			SolrInputField newField) {
		for( Object value  : newField.getValues() ){
			existingField.addValue(value, 1.0f);
		}
	}
	
	/**
	 * It does what you think, but StringUtils.join only works on 
	 * ArrayLists, not actual Arrays.
	 * @param array
	 * @param s
	 * @return
	 */
	public static String join(String[] array, String s)
	{
	  int len = array.length;
	  if ( len == 0 ) return null;
	  StringBuilder sb = new StringBuilder();
	  sb.append( array[0] );
	  for ( int x=1; x < len; x++ )
	    sb.append(s).append(array[x]);
	  return sb.toString();
	}

	
	/**
	 * Any time a comma is followed by a character that is not a space, a
	 * space will be inserted.
	 */
	static Pattern commaFollowedByNonSpace = null;
	public static String insertSpaceAfterCommas( String s ) {
		if (commaFollowedByNonSpace == null)
			commaFollowedByNonSpace = Pattern.compile(",([^\\s])");
		return commaFollowedByNonSpace.matcher(s).replaceAll(", $1");
	}

	//from http://stackoverflow.com/questions/139076/how-to-pretty-print-xml-from-java	
	public static String prettyXMLFormat(String input, int indent) {
	    try {
	        Source xmlInput = new StreamSource(new StringReader(input));
	        StringWriter stringWriter = new StringWriter();
	        StreamResult xmlOutput = new StreamResult(stringWriter);
	        TransformerFactory transformerFactory = TransformerFactory.newInstance();
//        transformerFactory.setAttribute("indent-number", indent); //removed this line due to runtime exception
		    Transformer transformer = transformerFactory.newTransformer(); 
		    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		    transformer.transform(xmlInput, xmlOutput);
		    return xmlOutput.getWriter().toString();
	} catch (Exception e) {
	    throw new RuntimeException(e); // simple exception handling, please review it
	    }
	}
	
	public static String prettyXMLFormat(String input) {
	    return prettyXMLFormat(input, 2);
	}

    
    
}
