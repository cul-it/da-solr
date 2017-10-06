package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

public class IndexingUtilities {

	public static enum IndexQueuePriority {
		DATACHANGE("Data Change"),
		DATACHANGE_SECONDARY("Secondary Data Change"),
		CODECHANGE_PRIORITY2("Code Change 2"),
		CODECHANGE_PRIORITY3("Code Change 3"),
		CODECHANGE_PRIORITY4("Code Change 4"),
		NOT_RECENTLY_UPDATED("Refresh Old Solr Record");

		private String string;

		private IndexQueuePriority(String name) {
			string = name;
		}

		public String toString() { return string; }
	}

    /**
	 * Gets the MFHD IDs related to all the BIB IDs in bibIds 
     * @throws SQLException 
	 */
	public static Set<Integer> getHoldingsForBibs( Connection current, Set<Integer> bibIds) throws SQLException {
        Set<Integer> mfhdIds = new HashSet<>();        
		try (PreparedStatement pstmt = current.prepareStatement(
				"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_VOY.toString()+" WHERE bib_id = ?")) {
			for (Integer bibid : bibIds) {
				pstmt.setInt(1,bibid);
				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next())
						mfhdIds.add(rs.getInt(1));
				}
			}
		}
        return mfhdIds;
	}	

	/**
	 * bib_id is either already in CurrentDBTable.BIB_VOY and presumably in Solr, or it may have been
	 * newly inserted as suppressed. If the former, it needs to be queued for delete from Solr. If
	 * the latter, it can be safely ignored.
	 * @param current
	 * @param bib_id
	 * @throws SQLException
	 */
	public static void queueBibDelete(Connection current, int bib_id) throws SQLException {
		boolean inBIB_VOY = false;
		try (PreparedStatement bibVoyQStmt = current.prepareStatement(
				"SELECT record_date FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?")) {
			bibVoyQStmt.setInt(1, bib_id);
			try ( ResultSet rs = bibVoyQStmt.executeQuery() ) {
				while (rs.next())
					inBIB_VOY = true;
			}
		}
		if ( ! inBIB_VOY )
			return;
		try (PreparedStatement bibVoyDStmt = current.prepareStatement(
				"DELETE FROM "+CurrentDBTable.BIB_VOY+" WHERE bib_id = ?")) {
			bibVoyDStmt.setInt(1, bib_id);
			bibVoyDStmt.executeUpdate();
			addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.DELETE);
		}
	}
	public static void addBibToUpdateQueue(Connection current, Integer bib_id, DataChangeUpdateType type) throws SQLException {
		try (PreparedStatement bibQueueStmt = current.prepareStatement(
				"INSERT INTO "+CurrentDBTable.QUEUE
				+" (bib_id, priority, cause)"
				+" VALUES (?, ?, ?)")) {
			bibQueueStmt.setInt(1, bib_id);
			bibQueueStmt.setInt(2, type.getPriority().ordinal());
			bibQueueStmt.setString(3,type.toString());
			bibQueueStmt.executeUpdate();
		}
	}
	public static void removeBibsFromUpdateQueue( Connection current, Set<Integer> bib_ids)
			throws SQLException {
		try (PreparedStatement bibQueueDStmt = current.prepareStatement(
				"DELETE FROM "+CurrentDBTable.QUEUE
				+" WHERE bib_id = ? AND done_date = 0")) {
			for (Integer bib_id : bib_ids) {
				bibQueueDStmt.setInt(1, bib_id);
				bibQueueDStmt.addBatch();
			}
			bibQueueDStmt.executeBatch();
		}
	}

	public static void optimizeIndex( String solrCoreURL ) throws IOException, SolrServerException {
		System.out.println("Optimizing index at: "+solrCoreURL+".");
		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		System.out.println("\tstarting at: "+dateFormat.format(Calendar.getInstance().getTime()));
		try ( SolrClient solr = new HttpSolrClient( solrCoreURL ) ) {
			solr.optimize();
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
			urlPatterns = loadPatternMap("online_site_identifications.txt");
		List<String> identifiedSites = new ArrayList<>();
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
		return String.join(", ",identifiedSites);
	}
	public static Map<String,String> loadPatternMap(String filename) {
		URL url = ClassLoader.getSystemResource(filename);
		Map<String,String> patternMap = new LinkedHashMap<>();
		try {
			Path p = Paths.get(url.toURI());
			List<String> sites = Files.readAllLines(p, StandardCharsets.UTF_8);
			for (String site : sites) {
				String[] parts = site.split("\\t", 2);
				if (parts.length < 2)
					continue;
				patternMap.put(parts[0].toLowerCase(), parts[1]);
			}
		} catch (URISyntaxException e) {
			// This should never happen since the URI syntax is machine generated.
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Couldn't read config file for site identifications.");
			e.printStackTrace();
			System.exit(1);
		}
		return patternMap;
	}
	static Map<String,String> urlPatterns = null;

	@SuppressWarnings("unchecked")
	public static TitleMatchReference pullReferenceFields(SolrDocumentBase<?,?> doc) throws ParseException {
		TitleMatchReference ref = new TitleMatchReference();

		Collection<Object> bibid_display = doc.getFieldValues("bibid_display");
		String[] parts = ((String)bibid_display.iterator().next()).split("\\|", 2);
		ref.id = Integer.valueOf(parts[0]);
		ref.timestamp = new Timestamp(marcDateFormat.parse(parts[1]).getTime() );

		ref.format = String.join(",",doc.getFieldValues("format"));

		if (doc.containsKey("url_access_display")) {
			Collection<Object> urlJsons = doc.getFieldValues("url_access_json");
			ref.sites = identifyOnlineServices(urlJsons);
			if (ref.sites == null)
				ref.sites = "Online";
			if (urlJsons.size() == 1) {
				String urlJson = urlJsons.iterator().next().toString();
				try {
					JsonNode node = mapper.readTree(urlJson);
					if (node.isObject()) {
						ObjectNode obj = (ObjectNode) node;
						if ( obj.has("url")  )
							ref.url = obj.get("url").asText(); 
					}
				} catch (IOException e) {
					System.out.println( "IOException: Failed to parse json with"
							+ " the same library that generated it. ("+e.getMessage()+")" );
					System.out.println(urlJson);
				}
			}
		}

		if (doc.containsKey("location_facet")) {
			Collection<String> libraries = new LinkedHashSet<>();
			libraries.addAll(doc.getFieldValues("location_facet"));
			ref.libraries = String.join(", ",libraries);
		}

		if (doc.containsKey("edition_display"))
			ref.edition = (String)doc.getFieldValues("edition_display").iterator().next();

		if (doc.containsKey("pub_date_display"))
			ref.pub_date = String.join(", ",doc.getFieldValues("pub_date_display"));

		if (doc.containsKey("language_facet"))
			ref.language = String.join(",",doc.getFieldValues("language_facet"));

		if (doc.containsKey("title_uniform_display")) {
			String uniformTitle = (String)doc.getFieldValues
					("title_uniform_display").iterator().next();
			int pipePos = uniformTitle.indexOf('|');
			if (pipePos == -1)
				ref.title = uniformTitle;
			else
				ref.title = uniformTitle.substring(0, pipePos);
		}
		if (ref.title == null && doc.containsKey("title_vern_display"))
			ref.title = (String)doc.getFieldValues("title_vern_display").iterator().next();
		if (ref.title == null && doc.containsKey("title_display"))
			ref.title = (String)doc.getFieldValues("title_display").iterator().next();

		return ref;
	}
	private static final ObjectMapper mapper = new ObjectMapper();
	public final static SimpleDateFormat marcDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	public static class TitleMatchReference {
		public int id;
		public String format = null;
		public String url = null;
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
		int cursor = s.length()-1;
		boolean isRightToLeft = s.endsWith(PDF_closeRTL);
		if (isRightToLeft)
			cursor -= PDF_closeRTL.length();
		while (cursor >= 0 && unwantedChars.indexOf( s.charAt(cursor) ) != -1)
			cursor--;

		boolean needsPeriod = false;
		// Ends with an initial, so keep the period
		if (cursor >= 1 && Character.isUpperCase(s.charAt(cursor)) 
				&& ! Character.isJavaIdentifierPart(s.charAt(cursor-1))
				&& s.charAt(cursor-1) != '-')
			needsPeriod = true;
		// Ends with etc., so keep the period
		else if (cursor >= 2 && s.substring(cursor-2, cursor+1).equals("etc") )
			needsPeriod = true;

		if (isRightToLeft) {
			if (needsPeriod) return s.substring(0, cursor+1)+'.'+PDF_closeRTL;
			return s.substring(0, cursor+1)+PDF_closeRTL;
		}
		if (needsPeriod) return s.substring(0, cursor+1)+'.';
		return s.substring(0, cursor+1);
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
		try (   GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(d));
				FileInputStream in = new FileInputStream(s)   ) {
			int bytes_read;
			while ((bytes_read = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytes_read);
			}
			out.finish();
		}
		FileUtils.deleteQuietly(new File(s));
	}

	public static SolrInputDocument xml2SolrInputDocument(String xml) throws XMLStreamException {
		SolrInputDocument doc = new SolrInputDocument();
		XMLInputFactory input_factory = XMLInputFactory.newInstance();
		XMLStreamReader r  = 
				input_factory.createXMLStreamReader(new StringReader(xml));
		while (r.hasNext()) {
			if (r.next() == XMLStreamConstants.START_ELEMENT) {
				if (r.getLocalName().equals("doc")) {
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("boost"))
							doc.setDocumentBoost(Float.valueOf(r.getAttributeValue(i)));
				} else if (r.getLocalName().equals("field")) {
					String fieldName = null;
					Float boost = null;
					for (int i = 0; i < r.getAttributeCount(); i++)
						if (r.getAttributeLocalName(i).equals("name"))
							fieldName = r.getAttributeValue(i);
						else if (r.getAttributeLocalName(i).equals("boost"))
							boost = Float.valueOf(r.getAttributeValue(i));
					if (boost != null)
						doc.addField(fieldName, r.getElementText(), boost);
					else
						doc.addField(fieldName, r.getElementText());
				}
			}
		}
		return doc;
	}
	
	//from http://stackoverflow.com/questions/139076/how-to-pretty-print-xml-from-java	
	public static String prettyXMLFormat(String input) {
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

}
