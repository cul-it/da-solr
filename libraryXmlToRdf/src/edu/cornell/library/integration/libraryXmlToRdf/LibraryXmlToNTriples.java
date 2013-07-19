package edu.cornell.library.integration.libraryXmlToRdf;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

public class LibraryXmlToNTriples {
	
	public static Collection<Integer> foundRecs = new HashSet<Integer>();
	public static Collection<Integer> suppressedRecs = new HashSet<Integer>();
	public static Collection<Integer> unsuppressedRecs = new HashSet<Integer>();
	public static Long recordCount = new Long(0);
	public static Collection<Integer> no245a = new HashSet<Integer>();
	public static Map<String,String> priority_libraries = new HashMap<String,String>();
	public static Map<String,String> libraries = new HashMap<String,String>();

	public static String uri_host = "http://da-rdf.library.cornell.edu/individual/";
	public static String integration_prefix = "http://da-rdf.library.cornell.edu/integrationLayer/0.1/";
	public static String label_p = "<http://www.w3.org/2000/01/rdf-schema#label>";
	public static String type_p = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";


	public static void libraryXmlToNTriples(File xmlfile, File target) {
		// priority libraries will match in preference to libraries on the main list.
		priority_libraries.put("Annex", "Library Annex");
		priority_libraries.put("Bailey", "Bailey Hortorium");
		priority_libraries.put("Kheel", "ILR Library Kheel Center");
		priority_libraries.put("Manuscripts", "Kroch Library Rare & Manuscripts");
		
		libraries.put("Adelson", "Adelson Library");
		libraries.put("Africana", "Africana Library");
		libraries.put("CISER", "CISER Data Archive");
		libraries.put("Engineering", "Ag Engineering Library");
		libraries.put("Entomology", "Entomology Library");
		libraries.put("Fine", "Fine Arts Library");
		libraries.put("Food", "Food Science Library");
		libraries.put("Geneva", "Geneva Library");
		libraries.put("ILR", "ILR Library");
		libraries.put("Kroch", "Kroch Library Asia");
		libraries.put("Law", "Law Library");
		libraries.put("Legal","Law Library"); // legal aid clinic
		libraries.put("Management", "Sage Hall Management Library");
		libraries.put("Mann","Mann Library");
		libraries.put("Mathematics","Mathematics Library");
		libraries.put("Mountain", "Iron Mountain");
		libraries.put("Music", "Music Library");
		libraries.put("Nestle", "Nestle Library");
		libraries.put("Olin", "Olin Library");
		libraries.put("Physical","Physical Sciences Library");
		libraries.put("Uris","Uris Library");
		libraries.put("Veterinary", "Veterinary Library");
		
		System.out.println(generateNTriples(priority_libraries));
		System.out.println(generateNTriples(libraries));
		
		try {
			FileInputStream xmlstream = new FileInputStream( xmlfile );
			XMLInputFactory input_factory = XMLInputFactory.newInstance();
			XMLStreamReader r  = 
					input_factory.createXMLStreamReader(xmlfile.getPath(), xmlstream);
			BufferedOutputStream out = null;
			out =  new BufferedOutputStream(new FileOutputStream(target, true));
					
			while (r.hasNext()) {
				String event = getEventTypeString(r.next());
				if (event.equals("START_ELEMENT"))
					if (r.getLocalName().equals("location")) {
						Location l = processLocation(r);
						if (l.locationOpac.equals("Y"))
							System.out.println(generateNTriples(l));
						else 
							System.out.println("Location "+l.locationCode+" / " + 
										l.locationName + " is suppressed ("+l.mfhdCount+").");
					}
			}
			xmlstream.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String destfile = "/users/fbw4/git/integrationLayer/rdf/library.nt";
		File file = new File( "/users/fbw4/git/integrationLayer/rdf/sources/locations.xml" );

		libraryXmlToNTriples( file, new File(destfile));
		
	}
	
	private static String generateNTriples ( Map<String,String> libraries ) {
		StringBuilder sb = new StringBuilder();
		Iterator<String> i = libraries.keySet().iterator();
		while (i.hasNext()) {
			String val = i.next();
			String library_uri = "<" + uri_host + "lib_" + val.toLowerCase() + ">";
			sb.append(library_uri + " " + type_p + "<" + integration_prefix + "Library>.\n");
			sb.append(library_uri + " " + label_p + " " + libraries.get(val) + ".\n");
		}
		return sb.toString();
	}
 
	private static String generateNTriples ( Location l ) {
		StringBuilder sb = new StringBuilder();

		String location_uri = uri_host + "loc_" + l.locationId;
		sb.append("<"+location_uri+"> <"+integration_prefix+"code> \""+escapeForNTriples(l.locationCode)+"\".\n");
		sb.append("<"+location_uri+"> " + type_p +" <"+integration_prefix+"Location>.\n");
		if (null != l.locationDisplayName) {
			sb.append("<"+location_uri+"> " + label_p + " \"" + escapeForNTriples(l.locationDisplayName)+ "\".\n");
			Boolean found_match = false;
			Iterator<String> i = priority_libraries.keySet().iterator();
			while (i.hasNext()) {
				String val = i.next();
				if (l.locationDisplayName.contains(val)) {
					sb.append("<"+location_uri+"> <"+integration_prefix+"hasLibrary> <"+integration_prefix+"lib_"+val.toLowerCase()+">.\n");
					found_match = true;
					break;
				}
			}
			i = libraries.keySet().iterator();
			while (i.hasNext() && ! found_match) {
				String val = i.next();
				if (l.locationDisplayName.contains(val)) {
					sb.append("<"+location_uri+"> <"+integration_prefix+"hasLibrary> <"+integration_prefix+"lib_"+val.toLowerCase()+">.\n");
					found_match = true;
					break;
				}
			}
			if ( ! found_match ) {
				System.out.println("MAPPING OF LOCATION NAMES TO LIBRARY NAMES INCOMPLETE FOR: \""+l.locationDisplayName +"\".");
			}
		} else
			sb.append("<"+location_uri+"> " + label_p + " \"" + escapeForNTriples(l.locationName)+ "\".\n");
		
		
		return sb.toString();
	}
		
	public static String escapeForNTriples( String s ) {
		s = s.replaceAll("\\\\", "\\\\\\\\");
		s = s.replaceAll("\"", "\\\\\\\"");
		s = s.replaceAll("[\n\r]+", "\\\\n");
		s = s.replaceAll("\t","\\\\t");
		return s;
	}
		
	private static Location processLocation( XMLStreamReader r ) throws Exception {
		
		Location l = new Location();

		while (r.hasNext()) {
			String event = getEventTypeString(r.next());
			if (event.equals("END_ELEMENT")) {
				if (r.getLocalName().equals("location")) 
					return l;
			}
			if (event.equals("START_ELEMENT")) {
				if (r.getLocalName().equals("locationId")) {
					l.locationId = Integer.valueOf(r.getElementText());
				} else if (r.getLocalName().equals("locationName")) {
					l.locationName = r.getElementText();
				} else if (r.getLocalName().equals("locationDisplayName")) {
					l.locationDisplayName = r.getElementText();
				} else if (r.getLocalName().equals("locationCode")) {
					l.locationCode = r.getElementText();
				} else if (r.getLocalName().equals("suppressInOpac")) {
					l.suppressInOpac = r.getElementText();
				} else if (r.getLocalName().equals("mfhdCount")) {
					l.mfhdCount = Integer.valueOf(r.getElementText());
				} else if (r.getLocalName().equals("libraryId")) {
					l.libraryId = Integer.valueOf(r.getElementText());
				} else if (r.getLocalName().equals("locationOpac")) {
					l.locationOpac = r.getElementText();
				} else if (r.getLocalName().equals("locationSpineLabel")) {
					l.locationSpineLabel = r.getElementText();
				}
		
			}
		}
		return l;
	}
	
	private final static String getEventTypeString(int  eventType)
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
	
	static class Location {
		
		// Fields from location table in Voyager database
		public Integer locationId; // unique
		public String  locationName;
		public String  locationDisplayName;
		public String  locationCode;
		public String  suppressInOpac;
		public Integer mfhdCount;
		public Integer libraryId; // always "1", maps to CUL. 
		public String  locationOpac;
		public String  locationSpineLabel;
	}
	
}
