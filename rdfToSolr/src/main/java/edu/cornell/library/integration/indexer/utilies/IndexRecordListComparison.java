package edu.cornell.library.integration.indexer.utilies;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

public class IndexRecordListComparison {

	public Set<Integer> bibsInIndexNotVoyager = new HashSet<Integer>();
	public Set<Integer> bibsInVoyagerNotIndex = new HashSet<Integer>();
	public Map<Integer,Integer> mfhdsInIndexNotVoyager = new HashMap<Integer,Integer>();
	public Set<Integer> mfhdsInVoyagerNotIndex = new HashSet<Integer>();
	
	
	public void compare(String coreUrl, Path currentVoyagerBibList, Path currentVoyagerMfhdList) {

		Set<Integer> currentIndexBibList = new HashSet<Integer>();
		Map<Integer,Integer> currentIndexMfhdList = new HashMap<Integer,Integer>();

		//Compile lists of bibs and mfhds currently in Solr.
		try {
			URL queryUrl = new URL(coreUrl + "/select?q=id%3A*&wt=xml&indent=true&qt=standard&fl=id,holdings_display&rows=10000000");
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			InputStream in = queryUrl.openStream();
			XMLStreamReader reader  = inputFactory.createXMLStreamReader(in);
			while (reader.hasNext()) {
				String event = getEventTypeString(reader.next());
				if (event.equals("START_ELEMENT"))
					if (reader.getLocalName().equals("doc"))
						processDoc(reader,currentIndexBibList,currentIndexMfhdList);
			}
			in.close();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
		
		// compare current index bib list with current voyager bib list
		// HashSet currentIndexBibList is NOT PRESERVED
		try {
			String line;
			BufferedReader reader = Files.newBufferedReader(currentVoyagerBibList, Charset.forName("US-ASCII"));
			while ((line = reader.readLine()) != null) {
				Integer bibid = Integer.valueOf(line);
				if (currentIndexBibList.contains(bibid)) {
					// bibid is on both lists.
					currentIndexBibList.remove(bibid);
				} else {
					bibsInVoyagerNotIndex.add(bibid);
				}
			}
			bibsInIndexNotVoyager.addAll(currentIndexBibList);
			currentIndexBibList.clear();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		// compare current index mfhd list with current voyager mfhd list
		// HashMap currentIndexMfhdList is NOT PRESERVED
		try {
			String line;
			BufferedReader reader = Files.newBufferedReader(currentVoyagerMfhdList, Charset.forName("US-ASCII"));
			while ((line = reader.readLine()) != null) {
				Integer mfhdid = Integer.valueOf(line);
				if (currentIndexMfhdList.containsKey(mfhdid)) {
					// mfhdid is on both lists.
					currentIndexMfhdList.remove(mfhdid);
				} else {
					mfhdsInVoyagerNotIndex.add(mfhdid);
				}
			}
			mfhdsInIndexNotVoyager.putAll(currentIndexMfhdList);
			currentIndexMfhdList.clear();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void processDoc( XMLStreamReader r, 
								   Set<Integer> currentIndexBibList,
								   Map<Integer,Integer> currentIndexMfhdList ) {
		Integer bibid = 0;
		HashSet<Integer> mfhdid = new HashSet<Integer>();
		String currentField = "";
		try {
			while (r.hasNext()) {
				int eventType = r.next();
				if (eventType == XMLEvent.END_ELEMENT) {
					if (r.getLocalName().equals("doc")) {
						// end of doc;
						if (bibid == 0) return;
						currentIndexBibList.add(bibid);
						Iterator<Integer> i = mfhdid.iterator();
						while (i.hasNext())
							currentIndexMfhdList.put(i.next(), bibid);
						return;
					}
				} else if (eventType == XMLEvent.START_ELEMENT) {
					if (r.getLocalName().equals("str") || r.getLocalName().equals("arr")) {
						for (int i = 0; i < r.getAttributeCount(); i++)
							if (r.getAttributeLocalName(i).equals("name"))
								currentField = r.getAttributeValue(i);
					}
					if (r.getLocalName().equals("str")) {
						if (currentField.equals("id"))
							bibid = Integer.valueOf(r.getElementText());
						else if (currentField.equals("holdings_display"))
							mfhdid.add(Integer.valueOf(r.getElementText()));
					}
				}
			}
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
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
	
}
