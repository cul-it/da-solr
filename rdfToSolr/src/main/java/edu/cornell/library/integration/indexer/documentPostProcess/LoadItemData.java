package edu.cornell.library.integration.indexer.documentPostProcess;

import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;
import edu.cornell.library.integration.indexer.fieldMaker.StandardMARCFieldMaker;
import edu.cornell.library.integration.indexer.resultSetToFields.NameFieldsAsColumnsRSTF;

/** If holdings record ID(s) are identified in holdings_display, pull any matching
 *  item records from the voyager database and serialize them in json.
 *  */
public class LoadItemData implements DocumentPostProcess{

	final static Boolean debug = false;
	Map<Integer,Location> locations = new HashMap<Integer,Location>();
	
	@Override
	public void p(String recordURI, SolrBuildConfig config,
			SolrInputDocument document) throws Exception {

		Connection conn = null;
		try {
			conn = config.getDatabaseConnection("Voy");
			loadItemData(document,conn, recordURI, config);
		} finally {
			if (conn != null) conn.close();
		}
	}
	
	private void loadItemData(SolrInputDocument document, Connection conn, String recordURI, SolrBuildConfig config) throws Exception {
		
		Boolean multivol = false;

		SolrInputField multivolField = new SolrInputField("multivol_b");

		if (! document.containsKey("holdings_display")) {
			multivolField.setValue(multivol, 1.0f);
			document.put("multivol_b", multivolField);
			return;
		}
		
		Map<String,LocationEnumStats> enumStats = new HashMap<String,LocationEnumStats>();
		SolrInputField holdingsField = document.getField( "holdings_record_display" );
		SolrInputField itemField = new SolrInputField("item_record_display");
		SolrInputField itemlist = new SolrInputField("item_display");
		ObjectMapper mapper = new ObjectMapper();
		for (Object hold_obj: holdingsField.getValues()) {

			@SuppressWarnings("unchecked")
			Map<String,Object> holdRec = mapper.readValue(hold_obj.toString(),Map.class);
			String mfhd_id = holdRec.get("id").toString();
			
			if (debug)
				System.out.println(mfhd_id);
			String query = "SELECT CORNELLDB.MFHD_ITEM.*, CORNELLDB.ITEM.*, CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME, CORNELLDB.ITEM_BARCODE.ITEM_BARCODE " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM_TYPE, CORNELLDB.ITEM" +
					" LEFT OUTER JOIN CORNELLDB.ITEM_BARCODE ON CORNELLDB.ITEM_BARCODE.ITEM_ID = CORNELLDB.ITEM.ITEM_ID " +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + mfhd_id + "\'" +
					   " AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
					   " AND CORNELLDB.ITEM.ITEM_TYPE_ID = CORNELLDB.ITEM_TYPE.ITEM_TYPE_ID" +
					   " AND CORNELLDB.ITEM_BARCODE.BARCODE_STATUS = '1'";
			if (debug)
				System.out.println(query);

	        Statement stmt = null;
	        ResultSet rs = null;
	        ResultSetMetaData rsmd = null;
	        try {
	        	stmt = conn.createStatement();
	
	        	rs = stmt.executeQuery(query);
	        	rsmd = rs.getMetaData();
	        	int mdcolumnCount = rsmd.getColumnCount();
	        	while (rs.next()) {
		        	   
	        		Map<String,Object> record = new HashMap<String,Object>();
	        		record.put("mfhd_id",mfhd_id);
	        		String loc = null;
	        		
	        		if (debug) 
	        			System.out.println();
	        		for (int i=1; i <= mdcolumnCount ; i++) {
	        			String colname = rsmd.getColumnName(i).toLowerCase();
	        			int coltype = rsmd.getColumnType(i);
	        			String value = null;
	       				if (coltype == java.sql.Types.CLOB) {
	       					Clob clob = rs.getClob(i);  
	        				value = convertClobToString(clob);
	        			} else { 
	        				value = rs.getString(i);
	       				}
	       				if (value == null)
	       					value = "";
	       				value = value.trim();
	       				if ((colname.equals("temp_location")
	       						|| colname.equals("perm_location"))
	       						&& ! value.equals("0")) {
	       					if (debug)
	       						System.out.println(colname+": "+value);
	       					Location l = getLocation(recordURI,config,Integer.valueOf(value));
	       					record.put(colname, l);
	       					if (colname.equals("perm_location")) loc = l.code;
	       				} else {
	       					record.put(colname, value);
	       					if (debug)
	       						System.out.println(colname+": "+value);
	       				}
	        		}
	        		
	        		if (loc != null) {
		        		LocationEnumStats stats = null;
		        		if (enumStats.containsKey(loc))
		        			stats = enumStats.get(loc);
		        		else
		        			stats = new LocationEnumStats();
		        		
		        		if (! stats.diverseEnumFound || ! stats.blankEnumFound ) {
		        			String enumeration = record.get("item_enum").toString() + 
		        					record.get("chron") + record.get("year");
		        			enumeration.replaceAll("c\\.[\\d+]", "");
		        			if (stats.aFoundEnum == null)
		        				stats.aFoundEnum = enumeration;
		        			if (! stats.aFoundEnum.equals(enumeration))
		        				stats.diverseEnumFound = true;
	
		        			if (enumeration.equals(""))
		        				stats.blankEnumFound = true;
		        			else if (stats.aFoundEnum.equals(""))
		        				stats.aFoundEnum = enumeration;
		        			
		        		}
		        		enumStats.put(loc, stats);
	        		}
	        		
	        		String json = mapper.writeValueAsString(record);
	        		if (debug)
	        			System.out.println(json);
	        		itemField.addValue(json, 1);
	        	}
	        } catch (SQLException ex) {
	           System.out.println(query);
	           System.out.println(ex.getMessage());
	        } catch (Exception ex) {
	        	ex.printStackTrace();
	        } finally {
	       
	           try {
	              if (stmt != null) stmt.close();
	              if (rs != null) rs.close();
	           } catch (Exception ex) {}
	        }
        
		}
		// if we have diverse enumeration within a single loc, it's a multivol
		Boolean blankEnum = false;
		Boolean nonBlankEnum = false;
		for (String loc : enumStats.keySet()) {
			LocationEnumStats l = enumStats.get(loc);
			if (l.diverseEnumFound) multivol = true;
			if (l.blankEnumFound) blankEnum = true;
			if ( ! l.aFoundEnum.equals("")) nonBlankEnum = true;
		}
		
		if (!multivol && enumStats.size() > 1) {
			Collection<String> nonBlankEnums = new HashSet<String>();
			for (LocationEnumStats stats : enumStats.values())
				if (! stats.aFoundEnum.equals(""))
					if ( ! nonBlankEnums.contains(stats.aFoundEnum))
						nonBlankEnums.add(stats.aFoundEnum);
			// nonBlankEnums differ between locations
			if (nonBlankEnums.size() > 1)
				multivol = true;
			
			// enumeration is consistent across locations
			else if ( ! blankEnum )
				multivol = false;
		}

		if (blankEnum && nonBlankEnum) {
			// We want to separate the cases where:
			//   1) this is a multivol where one item was accidentally not enumerated.
			//   2) this is a single volume work which is enumerated in one location 
			//               and not the other
			//   3) this is a single volume work with supplementary material, and the
			//               item lacking enumeration is the main item
			Boolean descriptionLooksMultivol = doesDescriptionLookMultivol(document);
			Boolean descriptionHasE = doesDescriptionHaveE(recordURI,config);
			if (descriptionHasE) {
				// this is strong evidence for case 3
				if (descriptionLooksMultivol == null || ! descriptionLooksMultivol) {
					// confirm case 3
					multivol = true;
				    SolrInputField t = new SolrInputField("mainitem_b");
				    t.setValue(true, 1.0f);
				    document.put("mainitem_b",t);
				} else {
					// multivol with an e? Not sure here, but concluding case 1
					multivol = true;
				    SolrInputField t = new SolrInputField("enumerror_b");
				    t.setValue(true, 1.0f);
				    document.put("enumerror_b",t);
				}
			} else {
				SolrInputField f = document.get("format");
				boolean solved = false;
				for (Object o : f.getValues()) 
					if (o.toString().equals("Journal")) {
						//concluding case 1 for now
						multivol = true;
					    SolrInputField t = new SolrInputField("enumerror_b");
					    t.setValue(true, 1.0f);
					    document.put("enumerror_b",t);
					    solved = true;
					}
				if (! solved && multivol) {
					// if there's no e but we have identified the work as a multivol already
					// (due to enumeration diversity), the conclude 3, but flag it.
				    SolrInputField t = new SolrInputField("enumerror_b");
				    t.setValue(true, 1.0f);
				    document.put("enumerror_b",t);
				    t = new SolrInputField("mainitem_b");
				    t.setValue(true, 1.0f);
				    document.put("mainitem_b",t);
				    solved = true;
				}
				if (! solved) {
					// not known to be a multivol, has no 300e, isn't a journal
					// conclude 2 for now.
				}
			}
		}

		
		if (blankEnum && multivol) {
			SolrInputField multivolWithBlank = new SolrInputField("multivolwblank_b");
			multivolWithBlank.setValue(true, 1.0f);
			document.put("multivolwblank_b", multivolWithBlank);
		}
		
		multivolField.setValue(multivol, 1.0f);
		document.put("multivol_b", multivolField);
		document.put("item_display", itemlist);
		document.put("item_record_display", itemField);
	}

	private static Pattern multivolDesc = null;
	private static Pattern singlevolDesc = null;
	private Boolean doesDescriptionLookMultivol(SolrInputDocument document) {
		if (! document.containsKey("description_display"))
			return null;
		SolrInputField f = document.getField("description_display");
		if (f.getValueCount() == 0)
			return null;
		// The first value will be 300 field, so that's the only one we care about.
		String desc = f.iterator().next().toString();
		if (multivolDesc == null) multivolDesc = Pattern.compile("(\\d+)\\D* v\\.");
		Matcher m = multivolDesc.matcher(desc);
		if (m.find()) {
			int c = Integer.valueOf(m.group(1).replaceAll("[^\\d\\-\\.]", ""));
			if (c > 1) return true;
			if (c == 1) return false;
		}
		if (singlevolDesc == null) singlevolDesc = Pattern.compile("^([^0-9\\-\\[\\]  ] )?p\\.");
		if (singlevolDesc.matcher(desc).find()) return false;

		
		return null;
	}

	private Boolean doesDescriptionHaveE(String recordURI,
			SolrBuildConfig config) throws Exception {
		
		StandardMARCFieldMaker fm = new StandardMARCFieldMaker("supp","300","e");
		Map<? extends String, ? extends SolrInputField> tempfields = 
				fm.buildFields(recordURI, config);
		return (tempfields.containsKey("supp"));
	}

	private Location getLocation (String recordURI, SolrBuildConfig config,
			Integer id) throws Exception{

		if (locations.containsKey(id))
			return locations.get(id);
		
		String loc_uri = "<http://da-rdf.library.cornell.edu/individual/loc_"+id+">";
		if (debug)
			System.out.println(loc_uri);
		SPARQLFieldMakerImpl fm = new SPARQLFieldMakerImpl().
        setName("location").
        addMainStoreQuery("location",
		   	"SELECT DISTINCT ?name ?library ?code \n"+
		   	"WHERE {\n"+
		    "  "+loc_uri+" rdf:type intlayer:Location.\n" +
		    "  "+loc_uri+" intlayer:code ?code.\n" +
		    "  "+loc_uri+" rdfs:label ?name.\n" +
		    "  OPTIONAL {\n" +
		    "    "+loc_uri+" intlayer:hasLibrary ?liburi.\n" +
		    "    ?liburi rdfs:label ?library.\n" +
		    "}}").
        addResultSetToFields( new NameFieldsAsColumnsRSTF());
		Map<? extends String, ? extends SolrInputField> tempfields = 
				fm.buildFields(recordURI, config);
		if (debug) {
			System.out.println(tempfields.toString());
			System.out.println(tempfields.get("name").getValue());
		}
		Location l = new Location();
		l.code = tempfields.get("code").getValue().toString();
		l.name = tempfields.get("name").getValue().toString();
		if (tempfields.containsKey("library"))
			l.library = tempfields.get("library").getValue().toString();
		else
			System.out.println("No location found for loc code "+l.code+".");
		l.number = id;
		locations.put(id, l);
		return l;
	}
	
    private static String convertClobToString(Clob clob) throws Exception {
        InputStream inputStream = clob.getAsciiStream();
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "utf-8");
        return writer.toString();
     }
	
	private static class Location {
		public String code;
		public Integer number;
		public String name;
		public String library;
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("code: ");
			sb.append(this.code);
			sb.append("; number: ");
			sb.append(this.number);
			sb.append("; name: ");
			sb.append(this.name);
			sb.append("; library: ");
			sb.append(this.library);
			return sb.toString();
		}
	}
	
	private static class LocationEnumStats {
		public String aFoundEnum = null;
		public Boolean blankEnumFound = false;
		public Boolean diverseEnumFound = false;
	}


}
