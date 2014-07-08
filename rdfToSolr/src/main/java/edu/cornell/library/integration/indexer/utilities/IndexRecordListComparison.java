package edu.cornell.library.integration.indexer.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
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

import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * This class is intended to compare the current voyager BIB and MFHD
 * records with the records in a Solr index. It will create a list of 
 * records that have been removed from Voyager or that are missing 
 * from the Solr Index.
 * 
 * To use this class, make a new IndexRecordListComparison and then
 * call compare().  After the call to compare() the properties
 * bibsInIndexNotVoyager, bibsInVoyagerNotIndex, mfhdsInIndexNotVoyager
 * and mfhdsInVoyagerNotIndex will then be set.
 * 
 * Maximum of 10,000,000 records will be returned from Solr. 
 */
public class IndexRecordListComparison {
    
	public Set<Integer> bibsInIndexNotVoyager = new HashSet<Integer>();
	public Set<Integer> bibsInVoyagerNotIndex = new HashSet<Integer>();
	public Map<Integer,Integer> mfhdsInIndexNotVoyager = new HashMap<Integer,Integer>();
	public Set<Integer> mfhdsInVoyagerNotIndex = new HashSet<Integer>();
	private static ObjectMapper jsonMapper = new ObjectMapper();

	/**
	 * Query Solr service at coreUrl and create a list:
	 * 
	 *  BIB records in Index but not in Voyager 
	 *  BIB records in Voyager but not in Index
	 *  MFHD records in Index but not in Voyager
	 *  MFHD records in Voyager but not in Index
	 *  
	 *  This method works by side-effect. 
	 *  
	 *  @param solrCoreURL URL of the solr instance to get the complement of.
	 *  @param currentVoyagerBibList file of BIB IDs in Voyager. Should have one BIB ID per line.
	 *  @param currentVoyagerMfhdList file of MFHD IDs in Voyager. Should have one MFHD ID per line.    
	 * @throws Exception 
	 */
	public void compare(String solrCoreURL, Path currentVoyagerBibList, Path currentVoyagerMfhdList) throws Exception {

		Set<Integer> solrIndexBibList = new HashSet<Integer>();
		Map<Integer,Integer> solrIndexMfhdList = new HashMap<Integer,Integer>();
		Map<Integer,Item> solrIndexItemList = new HashMap<Integer,Item>();

		//Compile lists of BIB and MFHD ids in Solr.
		try {
			URL queryUrl = new URL(solrCoreURL + "/select?q=id%3A*&wt=xml&indent=true&qt=standard&fl=id,holdings_display,item_display&rows=10000000");
			XMLInputFactory inputFactory = XMLInputFactory.newInstance();
			InputStream in = queryUrl.openStream();
			XMLStreamReader reader  = inputFactory.createXMLStreamReader(in);
			while (reader.hasNext()) {
				String event = getEventTypeString(reader.next());
				if (event.equals("START_ELEMENT"))
					if (reader.getLocalName().equals("doc"))
						processDoc(reader,solrIndexBibList,solrIndexMfhdList, solrIndexItemList);
			}
			in.close();
			System.out.println("Current index contains:");
			System.out.println("\tbib records: "+solrIndexBibList.size());
			System.out.println("\tmfhd records: "+solrIndexMfhdList.size());
			System.out.println("\titem recourds: "+solrIndexItemList.size());
		} catch( Exception e){
		    throw new Exception("Could not query Solr and/or parse the results. Solr URL: " + solrCoreURL, e);
		}
		
		// compare current index bib list with current voyager bib list		
		// HashSet solrIndexBibList is NOT PRESERVED		
		try {
			int unsuppressedBibCount = 0;
			String line;
			BufferedReader reader = Files.newBufferedReader(currentVoyagerBibList, Charset.forName("US-ASCII"));
			while ((line = reader.readLine()) != null) {
				Integer bibid = Integer.valueOf(line);
				unsuppressedBibCount++;
				if (solrIndexBibList.contains(bibid)) {
					// bibid is on both lists.
					solrIndexBibList.remove(bibid);
				} else {
					bibsInVoyagerNotIndex.add(bibid);
				}
			}
			System.out.println("unsuppressed bib record list contains: "+unsuppressedBibCount+" records");
			System.out.println("\ton list but not in index: "+bibsInVoyagerNotIndex.size());
			bibsInIndexNotVoyager.addAll(solrIndexBibList);
			System.out.println("\tin index but not on list: "+bibsInIndexNotVoyager.size());
			solrIndexBibList.clear();
			reader.close();
		} catch (IOException e) {
			throw new Exception("Could not read list of BIB IDs from file " 
			        + currentVoyagerBibList.toString(),e );
		}

		
		// compare current index mfhd list with current voyager mfhd list
		// HashMap solrIndexMfhdList is NOT PRESERVED
		try {
			int unsuppressedMfhdCount = 0;
			String line;
			BufferedReader reader = Files.newBufferedReader(currentVoyagerMfhdList, Charset.forName("US-ASCII"));
			while ((line = reader.readLine()) != null) {
				Integer mfhdid = Integer.valueOf(line);
				unsuppressedMfhdCount++;
				if (solrIndexMfhdList.containsKey(mfhdid)) {
					// mfhdid is on both lists.
					solrIndexMfhdList.remove(mfhdid);
				} else {
					mfhdsInVoyagerNotIndex.add(mfhdid);
				}
			}
			System.out.println("unsuppressed mfhd record list contains: "+unsuppressedMfhdCount+" records");
			System.out.println("\ton list but not in index: "+mfhdsInVoyagerNotIndex.size());
			mfhdsInIndexNotVoyager.putAll(solrIndexMfhdList);
			System.out.println("\tin index but not on list: "+mfhdsInIndexNotVoyager.size());
			solrIndexMfhdList.clear();
			reader.close();
		} catch (IOException e) {
			throw new Exception("Exception while comparing MFHD list",e);
		}
		
	}
	
	/**
	 * Process XML from solr and build solrIndexBibList
	 * and solrIndexMfhdList.  
	 */
	@SuppressWarnings("unchecked")
	private void processDoc( XMLStreamReader r,
								   Set<Integer> solrIndexBibList,
								   Map<Integer,Integer> solrIndexMfhdList, 
								   Map<Integer,Item> solrIndexItemList ) {
		Integer bibid = 0;
		HashSet<Integer> mfhdid = new HashSet<Integer>();
		Set<Item> itemid = new HashSet<Item>();
		String currentField = "";
		try {
			while (r.hasNext()) {
				int eventType = r.next();
				if (eventType == XMLEvent.END_ELEMENT) {
					if (r.getLocalName().equals("doc")) {
						// end of doc;
						if (bibid == 0) return;
						solrIndexBibList.add(bibid);
						Iterator<Integer> i = mfhdid.iterator();
						while (i.hasNext())
							solrIndexMfhdList.put(i.next(), bibid);
						Iterator<Item> iter = itemid.iterator();
						while (iter.hasNext()) {
							Item item = iter.next();
							item.bibid = bibid;
							solrIndexItemList.put(item.item_id,item);
						}
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
							try {
								bibid = Integer.valueOf(r.getElementText());
							} catch (NumberFormatException e) {
								// Not a Voyager record. We may want to support this in the future, but not yet.
								return;
							}
						else if (currentField.equals("holdings_display"))
							mfhdid.add(Integer.valueOf(r.getElementText()));
						else if (currentField.equals("item_record_display")) {
							String s = r.getElementText();
							Map<String,Object> itemRecord = null;
							try {
								itemRecord = jsonMapper.readValue(s, Map.class);
							} catch (Exception e) {
							//} catch (JsonParseException | JsonMappingException | IOException e) {
								// really shouldn't be an issue, as the json in question doesn't have multiple
								// sources and should parse correctly. Generic catch saves importing exceptions.
								e.printStackTrace();
							}
							if (itemRecord != null) {
								Item item = new IndexRecordListComparison.Item();
								item.item_id = Integer.valueOf(itemRecord.get("item_id").toString());
								item.mfhd_id = Integer.valueOf(itemRecord.get("mfhd_id").toString());
								itemid.add(item);
							}
						}
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
	
	protected class Item {
		Integer item_id;
		Integer mfhd_id;
		Integer bibid;
	}
	
}
