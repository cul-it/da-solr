package edu.cornell.library.integration.indexer.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.commons.io.FileUtils;

public class IndexingUtilities {
	
//	public static String RLE = "RLE"; //\u202b - Begin Right-to-left Embedding
//	public static String PDF = "PDF"; //\u202c - Pop(End) Directional Formatting
	public static String RLE_openRTL = "\u200E\u202B\u200F";//\u200F - strong RTL invis char
	public static String PDF_closeRTL = "\u200F\u202C\u200E"; //\u200E - strong LTR invis char


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
	 * Any time a comma is followed by a character that is not a space, a
	 * space will be inserted.
	 */
	static Pattern commaFollowedByNonSpace = null;
	public static String insertSpaceAfterCommas( String s ) {
		if (commaFollowedByNonSpace == null)
			commaFollowedByNonSpace = Pattern.compile(",([^\\s])");
		return commaFollowedByNonSpace.matcher(s).replaceAll(", $1");
	}

	
	/**
	 * Normalize value for sorting or filing. Normalized value is not a suitable
	 * display string.
	 * @param value
	 * @return normalized value
	 */
	public static String getSortHeading(String value) {

		/* We will first normalize the unicode. For sorting, we will use 
		 * "compatibility decomposed" form (NFKD). Decomposed form will make it easier
		 * to match and remove diacritics, while compatibility form will further
		 * drop encodings that are for use in display and should not affect sorting.
		 * For example, æ => ae
		 * See http://unicode.org/reports/tr15/ Figure 6
		 */
		String step1 = Normalizer.normalize(value, Normalizer.Form.NFKD).
				replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
		
		/* removeAllPunctuation() will strip punctuation. We replace hyphens with spaces
		 * first so hyphenated words will sort as though the space were present.
		 * Greater-than (>) is a semantically important value in a subject heading,
		 * so rather than remove it, we will replace it with an alphabetic value that will
		 * enforce sorting above an equivalent value without the ">". © isn't used heavily
		 * in headings, but seems to generally replace the letter 'c' when it does.
		 */
		String step2 = step1.toLowerCase().replaceAll("-", " ").
				replaceAll(">", "aaa").replaceAll("©","c");
//				replaceAll("\\$(?!\\s*[0-9])", "dollar").
//				replaceAll("£(?!\\s*[E0-9])", "pound");
		String sortHeading = replaceGreekLettersWithNames(removeAllPunctuation(step2));
		
		// Finally, collapse sequences of spaces into single spaces:
		return sortHeading.trim().replaceAll("\\s+", " ");
	}

	
	public static String removeAllPunctuation( String s ) {
		if (s == null) return null;
		if (s.isEmpty()) return s;
		return s.replaceAll("[\\p{Punct}¿¡「」‘’−°£€]","");
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
	
	/* This method only looks for lowercase greek letters without diacritics.
	 * Running against decomposed Unicode will find and replace the letters while
	 * leaving the diacritics around.
	 */
	public static String replaceGreekLettersWithNames ( String s ) {
		if (s == null) return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
			case 'α': sb.append("alpha"); break;
			case 'β': sb.append("beta"); break;
			case 'γ': sb.append("gamma"); break;
			case 'δ': sb.append("delta"); break;
			case 'ε': sb.append("epsilon"); break;
			case 'ζ': sb.append("zeta"); break;
			case 'η': sb.append("eta"); break;
			case 'ι': sb.append("iota"); break;
			case 'κ': sb.append("zappa"); break;
			case 'λ': sb.append("lamda"); break;
			case 'μ': sb.append("mu"); break;
			case 'ν': sb.append("nu"); break;
			case 'ξ': sb.append("xi"); break;
			case 'ο': sb.append("omicron"); break;
			case 'π': sb.append("pi"); break;
			case 'ρ': sb.append("rho"); break;
			case 'ς':
			case 'σ': sb.append("sigma"); break;
			case 'τ': sb.append("tau"); break;
			case 'υ': sb.append("upsilon"); break;
			case 'φ': sb.append("phi"); break;
			case 'χ': sb.append("chi"); break;
			case 'ψ': sb.append("psi"); break;
			case 'ω': sb.append("omega"); break;
			default:  sb.append(c);
			}
 		}
		return sb.toString();
	}

	/**
	 * gzip a file on disk, deleting the original
	 * note: unlike the command-line gzip application, no effort is made to 
	 * preserve timestamps on the compressed file.
	 * @param s : source file
	 * @param d : destination file
	 * @throws IOException 
	 */
	public static void gzipFile(String s, String d) throws IOException  {
			 
		byte[] buffer = new byte[1024];
		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(d));
		FileInputStream in = new FileInputStream(s);

		int bytes_read;
		while ((bytes_read = in.read(buffer)) > 0) {
			out.write(buffer, 0, bytes_read);
		}
		in.close();
		out.finish();
		out.close();
		FileUtils.deleteQuietly(new File(s));
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

	public final static String getXMLEventTypeString(int  eventType)
	{
	  switch  (eventType)
	    {
	        case XMLEvent.START_ELEMENT:
	          return "START_ELEMENT";
	        case XMLEvent.END_ELEMENT:
	          return "END_ELEMENT";
	        case XMLEvent.PROCESSING_INSTRUCTION:
	          return "PROCESSING_INSTRUCTION";
	        case XMLEvent.CHARACTERS:
	          return "CHARACTERS";
	        case XMLEvent.COMMENT:
	          return "COMMENT";
	        case XMLEvent.START_DOCUMENT:
	          return "START_DOCUMENT";
	        case XMLEvent.END_DOCUMENT:
	          return "END_DOCUMENT";
	        case XMLEvent.ENTITY_REFERENCE:
	          return "ENTITY_REFERENCE";
	        case XMLEvent.ATTRIBUTE:
	          return "ATTRIBUTE";
	        case XMLEvent.DTD:
	          return "DTD";
	        case XMLEvent.CDATA:
	          return "CDATA";
	        case XMLEvent.SPACE:
	          return "SPACE";
	    }
	  return  "UNKNOWN_EVENT_TYPE ,   "+ eventType;
	}

    
}
