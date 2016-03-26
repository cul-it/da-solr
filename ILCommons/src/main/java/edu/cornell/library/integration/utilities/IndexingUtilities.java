package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;

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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

public class IndexingUtilities {

	public static enum IndexQueuePriority {
		DATACHANGE("Data Change"),
		CODECHANGE_PRIORITY1("Code Change 1"),
		CODECHANGE_PRIORITY2("Code Change 2"),
		CODECHANGE_PRIORITY3("Code Change 3"),
		CODECHANGE_PRIORITY4("Code Change 4");

		private String string;

		private IndexQueuePriority(String name) {
			string = name;
		}

		public String toString() { return string; }
	}

	public static void optimizeIndex( String solrCoreURL ) {
		System.out.println("Optimizing index at: "+solrCoreURL+".");
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

	/**
	 * 
	 * @param urls - a list of url_access_display values
	 * @return A comma-separated list of online provider names
	 */
	public static String identifyOnlineServices(Collection<Object> urls) {
		if (urlPatterns == null)
			loadUrlPatterns();
		List<String> identifiedSites = new ArrayList<String>();
		for (Object url_o : urls) {
			String url = url_o.toString().toLowerCase();
			for (Map.Entry<String, String> pattern : urlPatterns.entrySet())
				if (url.contains(pattern.getKey())) {
					if ( ! identifiedSites.contains(pattern.getValue()) )
						identifiedSites.add(pattern.getValue());
					break;
				}
		}
		if (identifiedSites.isEmpty())
			return null;
		return StringUtils.join(identifiedSites, ", ");
	}
	private static void loadUrlPatterns() {
		URL url = ClassLoader.getSystemResource("online_site_identifications.txt");
		urlPatterns = new HashMap<String,String>();
		try {
			Path p = Paths.get(url.toURI());
			List<String> sites = Files.readAllLines(p, StandardCharsets.UTF_8);
			for (String site : sites) {
				String[] parts = site.split("\\t", 2);
				if (parts.length < 2)
					continue;
				urlPatterns.put(parts[0].toLowerCase(), parts[1]);
			}
		} catch (URISyntaxException e) {
			// This should never happen since the URI syntax is machine generated.
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Couldn't read config file for site identifications.");
			e.printStackTrace();
			System.exit(1);
		}
	}
	static Map<String,String> urlPatterns = null;



	public static String eliminateDuplicateLocations(ArrayList<Object> location_facet) {
		if (location_facet == null) return "";
		StringBuilder sb = new StringBuilder();
		Collection<Object> foundValues = new HashSet<Object>();
		boolean first = true;
		for (Object val : location_facet) {
			if (foundValues.contains(val))
				continue;
			foundValues.add(val);
			if (first)
				first = false;
			else
				sb.append(", ");
			sb.append(val.toString());
		}
		return sb.toString();
	}

	@SuppressWarnings("unchecked")
	public static TitleMatchReference pullReferenceFields(SolrDocument doc) throws ParseException {
		TitleMatchReference ref = new TitleMatchReference();
		Object bibid_display = doc.getFieldValue("bibid_display");
		if (bibid_display.getClass().equals(String.class)) {
			String[] parts = ((String)bibid_display).split("\\|", 2);
			ref.id = Integer.valueOf(parts[0]);
			ref.timestamp = new Timestamp(marcDateFormat.parse(parts[1]).getTime() );
		} else {
			ArrayList<Object> solrBib = (ArrayList<Object>) bibid_display;
			String[] parts = ((String)solrBib.get(0)).split("\\|", 2);
			ref.id = Integer.valueOf(parts[0]);
			ref.timestamp = new Timestamp(marcDateFormat.parse(parts[1]).getTime() );
		}
		Object format = doc.getFieldValue("format");
		if (format.getClass().equals(String.class))
			ref.format = (String)format;
		else
			ref.format = StringUtils.join((ArrayList<Object>)format,',');
		boolean online = doc.containsKey("url_access_display");
		if (online) {
			Object url_access_display = doc.getFieldValue("url_access_display");
			if (url_access_display.getClass().equals(String.class)) {
				ArrayList<Object> urls = new ArrayList<Object>();
				urls.add(url_access_display);
				ref.sites = identifyOnlineServices(urls);
			} else {
				ref.sites = identifyOnlineServices((ArrayList<Object>)url_access_display);
			}
			if (ref.sites == null)
				ref.sites = "Online";
		}
		if (doc.containsKey("location_facet")) {
			Object location_facet = doc.getFieldValue("location_facet");
			if (location_facet.getClass().equals(String.class)) {
				ref.libraries = (String) location_facet;
			} else {
				ref.libraries = eliminateDuplicateLocations((ArrayList<Object>)location_facet);			
			}
		}
		if (doc.containsKey("edition_display")) {
			Object edition_display = doc.getFieldValue("edition_display");
			if (edition_display.getClass().equals(String.class)) {
				ref.edition = (String) edition_display;
			} else {
				ref.edition = (String)((ArrayList<Object>) edition_display).get(0);
			}
		}
		if (doc.containsKey("pub_date_display")) {
			Object pub_date_display = doc.getFieldValue("pub_date_display");
			if (pub_date_display.getClass().equals(String.class))
				ref.pub_date = (String) pub_date_display;
			else 
				ref.pub_date = StringUtils.join((ArrayList<Object>)pub_date_display ,", ");
		}
		if (doc.containsKey("language_facet")) {
			Object language_facet = doc.getFieldValue("language_facet");
			if (language_facet.getClass().equals(String.class))
				ref.language = (String) language_facet;
			else
				ref.language = StringUtils.join((ArrayList<Object>)language_facet,',');
		}
		if (doc.containsKey("title_uniform_display")) {
			Object title_uniform_display = doc.getFieldValue("title_uniform_display");
			String uniformTitle;
			if (title_uniform_display.getClass().equals(String.class))
				uniformTitle = (String) title_uniform_display;
			else 
				uniformTitle = (String)((ArrayList<Object>)title_uniform_display ).get(0);
			int pipePos = uniformTitle.indexOf('|');
			if (pipePos == -1)
				ref.title = uniformTitle;
			else
				ref.title = uniformTitle.substring(0, pipePos);
		}
		if (ref.title == null && doc.containsKey("title_vern_display"))
			ref.title = (String) doc.getFieldValue("title_vern_display");
		if (ref.title == null)
			ref.title = (String) doc.getFieldValue("title_display");
		return ref;
	}
	public final static SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	public static class TitleMatchReference {
		public int id;
		public String format = null;
		public String sites = null;
		public String libraries = null;
		public String edition = null;
		public String pub_date = null;
		public String language = null;
		public String title = null;
		public java.sql.Timestamp timestamp = null;
		public TitleMatchReference() {
		}
	}


	/**
	 *
	 * @param date
	 * @return If date consists of eight digits, then dashes are added to create a yyyy-mm-dd format.
	 *      Otherwise, the date is returned unchanged.
	 */
	public static String addDashesTo_YYYYMMDD_Date(String date) {
		if (yyyymmdd == null)
			yyyymmdd = Pattern.compile("^\\s*(\\d{4})(\\d{2})(\\d{2})\\s*$");
		Matcher m = yyyymmdd.matcher(date);
		if (m.find())
			return m.group(1)+"-"+m.group(2)+"-"+m.group(3);
		else
			return date;
	}
	static Pattern yyyymmdd = null;

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

	

	public static String removeTrailingPunctuation ( String s, String unwantedChars ) {
		if (s == null) return null;
		if (unwantedChars == null) return s;
		if (s.equals("")) return s;
		if (unwantedChars.equals("")) return s;
		Pattern p = Pattern.compile ("[" + unwantedChars + "]*("+PDF_closeRTL+"?)*$");
		Matcher m = p.matcher(s);
		return m.replaceAll("$1");
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
}