package edu.cornell.library.integration.utilities;

import static edu.cornell.library.integration.marc.DataField.PDF_closeRTL;
import static edu.cornell.library.integration.marc.MarcRecord.MARC_DATE_FORMAT;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocumentBase;
import org.apache.solr.common.SolrInputDocument;

public class IndexingUtilities {

	/**
	 * bib_id is either already in CurrentDBTable.BIB_VOY and presumably in Solr, or it may have been
	 * newly inserted as suppressed. If the former, it needs to be queued for delete from Solr. If
	 * the latter, it can be safely ignored.
	 * @param current
	 * @param bib_id
	 * @throws SQLException
	 */
	public static void queueBibDelete(Connection current, String hrid) throws SQLException {
		boolean inInstanceFolio = false;
		try (PreparedStatement instanceFolioQStmt = current.prepareStatement(
				"SELECT id FROM instanceFolio WHERE hrid = ?")) {
			instanceFolioQStmt.setString(1, hrid);
			try ( ResultSet rs = instanceFolioQStmt.executeQuery() ) {
				while (rs.next()) inInstanceFolio = true;
			}
		}
		if ( ! inInstanceFolio )
			return;
		try (PreparedStatement instanceFolioDStmt = current.prepareStatement(
				"DELETE FROM instanceFolio WHERE hrid = ?");
				PreparedStatement queueDeleteStmt = AddToQueue.deleteQueueStmt(current)) {
			instanceFolioDStmt.setString(1, hrid);
			instanceFolioDStmt.executeUpdate();
			queueDeleteStmt.setString(1,hrid);
			queueDeleteStmt.executeUpdate();
		}
	}

	/**
	 * 
	 * @param urls - a list of url_access_display values
	 * @return A comma-separated list of online provider names
	 */
	private static String identifyOnlineServices(Collection<Object> urls) {
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
	private static Map<String,String> urlPatterns = null;

	public static TitleMatchReference pullReferenceFields(SolrDocumentBase<?,?> doc) throws ParseException {
		TitleMatchReference ref = new TitleMatchReference();

		Collection<Object> bibid_display = doc.getFieldValues("bibid_display");
		String[] parts = ((String)bibid_display.iterator().next()).split("\\|", 2);
		ref.id = Integer.valueOf(parts[0]);
		ref.timestamp = new Timestamp((new SimpleDateFormat( MARC_DATE_FORMAT )).parse(parts[1]).getTime() );

		ref.format = String.join(",",doc.getFieldValues("format"));

		if (doc.containsKey("url_access_display")) {
			Collection<Object> urlJsons = doc.getFieldValues("url_access_json");
			ref.sites = identifyOnlineServices(urlJsons);
			if (ref.sites == null)
				ref.sites = "Online";
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
		return date;
	}
	private static Pattern yyyymmdd = null;

	/**
	 * Any time a comma is followed by a character that is not a space, a
	 * space will be inserted.
	 */
	private static Pattern commaFollowedByNonSpace = null;
	public static String insertSpaceAfterCommas( String s ) {
		if (commaFollowedByNonSpace == null)
			commaFollowedByNonSpace = Pattern.compile(",([^\\s])");
		return commaFollowedByNonSpace.matcher(s).replaceAll(", $1");
	}

	

	private static final List<String> tokensNeedingPeriods = Collections.unmodifiableList(
			Arrays.asList( "etc", "Jr", "Sr", "Inc", "Co" ));
	public static String removeTrailingPunctuation ( String s, String unwantedChars ) {
		if (s == null) return null;
		if (unwantedChars == null) return s;
		if (s.equals("")) return s;
		if (unwantedChars.equals("")) return s;
		int cursor = s.length()-1;
		boolean isRightToLeft = s.endsWith(PDF_closeRTL);
		if (isRightToLeft)
			cursor -= PDF_closeRTL.length();
		while (cursor >= 0 && unwantedChars.indexOf( s.charAt(cursor) ) != -1
				&& ! (cursor >= 2 && s.substring(cursor-2,cursor+1).equals("...") ))
			cursor--;

		boolean needsPeriod = false;
		// Ends with an initial, so keep the period
		if (cursor >= 1 && Character.isUpperCase(s.charAt(cursor)) 
				&& ! Character.isJavaIdentifierPart(s.charAt(cursor-1))
				&& s.charAt(cursor-1) != '-')
			needsPeriod = true;
		// Back up to find beginning of last token. If last token is on tokensNeedingPeriods, keep the period.
		else if (cursor > 0){
			int reverseCursor = cursor;
			while (reverseCursor > 0
					&& Character.isJavaIdentifierPart(s.charAt(reverseCursor-1)))
				reverseCursor--;
			String lastToken = s.substring(reverseCursor,cursor+1);
			if (tokensNeedingPeriods.contains(lastToken))
				needsPeriod = true;
		}

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

}
