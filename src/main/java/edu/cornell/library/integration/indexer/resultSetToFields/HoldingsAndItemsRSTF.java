package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.utilities.IndexingUtilities.insertSpaceAfterCommas;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.voyager.Locations;
import edu.cornell.library.integration.voyager.Locations.Location;

/**
 * Collecting holdings data needed by the Blacklight availability service into holdings_record_display.
 * For a bib record with multiple holdings records, each holdings record will have it's own holdings_record_display.
 */
public class HoldingsAndItemsRSTF implements ResultSetToFields {

	private Boolean debug = false;
	static final Pattern p = Pattern.compile(".*_([0-9]+)");
	Collection<String> holding_ids = new TreeSet<>();
	Map<String,Holdings> holdings = new TreeMap<>();
	Map<String,SolrInputField> fields = new HashMap<>();
	Collection<String> descriptions = new HashSet<>();
	boolean description_with_e = false;
	String rectypebiblvl = null;
	Collection<Map<String,Object>> boundWiths = new ArrayList<>();
	Connection conn = null;
	static ObjectMapper mapper = new ObjectMapper();
	Locations locations;
	Collection<String> workLibraries = new HashSet<>();

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		Map<String,MarcRecord> recs = new HashMap<>();
		locations = new Locations(config);

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();

				if ( resultKey.equals("description")) {

					MarcRecord rec = new MarcRecord();
					rec.addDataFieldQuerySolution(sol);
					for (DataField f : rec.data_fields.values()) {
						descriptions.add(f.concatenateSubfieldsOtherThan6());
						for (Subfield sf : f.subfields.values())
							if (sf.code.equals('e'))
								description_with_e = true;
					}

				} else if ( resultKey.equals("rectypebiblvl") ) {

					rectypebiblvl = nodeToString(sol.get("rectypebiblvl"));

				} else {
	 				String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (recs.containsKey(recordURI)) {
						rec = recs.get(recordURI);
					} else {
						rec = new MarcRecord();
					}
					if (resultKey.contains("control")) {
						rec.addControlFieldQuerySolution(sol);
					} else if (resultKey.contains("data")) {
						rec.addDataFieldQuerySolution(sol);
					}
					recs.put(recordURI, rec);

				}
			}
//			rec.addDataFieldResultSet(rs);
		}

		for( String holdingURI: recs.keySet() ) {
			MarcRecord rec = recs.get(holdingURI);

			Collection<Location> holdingLocations = new HashSet<>();
			Collection<String> callnos = new HashSet<>();
			List<String> holdingDescs = new ArrayList<>();
			List<String> recentHoldings = new ArrayList<>();
			List<String> supplementalHoldings = new ArrayList<>();
			List<String> indexHoldings = new ArrayList<>();
			List<String> notes = new ArrayList<>();
			String copyNo = null;

			Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();

			Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				FieldSet fs = sortedFields.get(id);
				DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
				for (DataField f: dataFields) {
					String callno = null;
					switch (f.tag) {
					case "506":
						// restrictions on access note
						notes.add(f.concatenateSpecificSubfields("ancdefu3"));
						break;
					case "561":
						// ownership and custodial history
						if (! f.ind1.equals('0')) // '0' = private
							notes.add(f.concatenateSpecificSubfields("au3"));
						break;
					case "562":
						// copy and version identification note
						notes.add(f.concatenateSpecificSubfields("abcde3"));
						break;
					case "843":
						// reproduction note
						notes.add(f.concatenateSpecificSubfields("abcdefmn3"));
						break;
					case "845":
						// terms governing use and reproduction note
						notes.add(f.concatenateSpecificSubfields("abcdu3"));
						break;
					case "852":
						for (Subfield sf: f.subfields.values()) {
							CODE: switch (sf.code) {
							case 'b':
								Location l = locations.getByCode(sf.value.trim());
								if (l != null)
									holdingLocations.add(l);
								else
									System.out.println("location not identified for code='"+sf.value+"'.");
								break CODE;
							case 'h':
								// If there is a subfield ‡h, then there is a call number. So we will record
								// a concatenation of all the call number fields. If there are (erroneously)
								// multiple subfield ‡h entries in one field, the callno will be overwritten
								// and not duplicated in the call number array.
								callno = f.concatenateSpecificSubfields("hijklm"); break CODE;
							case 'z':
								notes.add(sf.value); break CODE;
							case 't':
								copyNo = sf.value; break CODE;
							}
						}
						break;
					case "866":
						if (f.ind1.equals(' ') && f.ind2.equals(' '))
							recentHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						else
							holdingDescs.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					case "867":
						supplementalHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					case "868":
						indexHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					case "876":
						registerBoundWith(config, rec.id, f);
					}
					if (callno != null)
						callnos.add(callno);
				}
			}

			Holdings holding = new Holdings();
			holding.id = rec.id;
			holding.copy_number = copyNo;
			if ( ! holdingLocations.contains(locations.getByCode("serv,remo")) ) {
				holding.callnos = callnos.toArray(new String[ callnos.size() ]);
			}
			holding.notes = notes.toArray(new String[ notes.size() ]);
			holding.holdings_desc = holdingDescs.toArray(new String[ holdingDescs.size() ]);
			holding.recent_holdings_desc = recentHoldings.toArray(new String[ recentHoldings.size() ]);
			holding.supplemental_holdings_desc = supplementalHoldings.toArray(new String[ supplementalHoldings.size() ]);
			holding.index_holdings_desc = indexHoldings.toArray(new String[ indexHoldings.size() ]);
			holding.locations = holdingLocations.toArray(new Location[ holdingLocations.size() ]);
			if (rec.modified_date != null) {
				if (rec.modified_date.length() >= 14)
					holding.modified_date = rec.modified_date.substring(0, 14);
				else
					holding.modified_date = rec.modified_date;
			}

			if (holding.modified_date != null)
				addField(fields,"holdings_display",holding.id+"|"+holding.modified_date);
			else
				addField(fields,"holdings_display",holding.id);
			ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream,holding);
			String json = jsonstream.toString("UTF-8");
			addField(fields,"holdings_record_display",json);
			holdings.put(holding.id, holding);
			holding_ids.add(holding.id);

		}
		if (debug) System.out.println("holdings found: "+StringUtils.join(", ", holding_ids));

		for (Map<String,Object> boundWith : boundWiths) {
			ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream, boundWith);
			String json = jsonstream.toString("UTF-8");
			addField(fields,"bound_with_json",json);
			addField(fields,"barcode_addl_t",boundWith.get("barcode").toString());
		}

		// ITEM DATA STARTS HERE
		try {
			if (conn == null)
				conn = config.getDatabaseConnection("Voy");
			loadItemData(conn);
		} finally {
			if (conn != null) conn.close();
		}

		for (String lib : workLibraries)
			addField(fields,"location_facet",lib);
		if ( ! workLibraries.isEmpty() )
			addField(fields,"online","At the Library");

		return fields;
	}

	private void registerBoundWith(SolrBuildConfig config, String mfhd_id, DataField f) throws Exception {
		String item_enum = "";
		String barcode = null;
		for (Subfield sf : f.subfields.values()) {
			switch (sf.code) {
			case 'p': barcode = sf.value; break;
			case '3': item_enum = sf.value; break;
			}
		}
		if (barcode == null) return;
		if (conn == null)
			conn = config.getDatabaseConnection("Voy");
		// lookup item id here!!!
		int item_id = 0;
		try (  Statement stmt = conn.createStatement() ){
			String query =
				"SELECT CORNELLDB.ITEM_BARCODE.ITEM_ID "
				+ "FROM CORNELLDB.ITEM_BARCODE WHERE CORNELLDB.ITEM_BARCODE.ITEM_BARCODE = '"+barcode+"'";
			try (  java.sql.ResultSet rs = stmt.executeQuery(query)  ){
				while (rs.next()) {
					item_id = rs.getInt(1);
				}
				if (item_id == 0) return;
			}
		}
		Map<String,Object> boundWith = new HashMap<>();
		boundWith.put("item_id", item_id);
		boundWith.put("mfhd_id", mfhd_id);
		boundWith.put("item_enum", item_enum);
		boundWith.put("barcode", barcode);
		boundWiths.add(boundWith);
	}

	private void loadItemData(Connection conn) throws Exception {
		
		Boolean multivol = false;
		SolrInputField multivolField = new SolrInputField("multivol_b");

		if (holdings.isEmpty()) {
			multivolField.setValue(multivol, 1.0f);
			fields.put("multivol_b", multivolField);
			return;
		}
		
		Map<String,LocationEnumStats> enumStats = new HashMap<>();
		Map<Integer,Map<String,Object>> items = new TreeMap<>();
		ObjectMapper mapper = new ObjectMapper();
		for (Holdings h : holdings.values()) {

			String holdingsLibrary = null;
			for (int i = 0; i < h.locations.length; i++) {
				Location l = h.locations[i];
				if (l.library != null)
					holdingsLibrary = l.library;
			}
        	boolean foundItems = false;

			if (debug)
				System.out.println(h.id);
			String query = 
					"SELECT CORNELLDB.MFHD_ITEM.*,"
					+ "     CORNELLDB.ITEM.*,"
					+ "     CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME,"
					+ "     CORNELLDB.ITEM_BARCODE.ITEM_BARCODE " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM_TYPE, CORNELLDB.ITEM" +
					" LEFT OUTER JOIN CORNELLDB.ITEM_BARCODE"
					+ "    ON CORNELLDB.ITEM_BARCODE.ITEM_ID = CORNELLDB.ITEM.ITEM_ID "+
					        " AND CORNELLDB.ITEM_BARCODE.BARCODE_STATUS = '1'" +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = '" + h.id + "'" +
					   " AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
					   " AND CORNELLDB.ITEM.ITEM_TYPE_ID = CORNELLDB.ITEM_TYPE.ITEM_TYPE_ID" ;
			if (debug)
				System.out.println(query);

	        ResultSetMetaData rsmd = null;
	        try (Statement stmt = conn.createStatement();
	        		java.sql.ResultSet rs = stmt.executeQuery(query)) {

	        	rsmd = rs.getMetaData();
	        	int mdcolumnCount = rsmd.getColumnCount();
	        	while (rs.next()) {

	        		Map<String,Object> record = new HashMap<>();
	        		record.put("mfhd_id",h.id);
	        		String loc = null;
	        		String tempLibrary = null;
	        		
	        		if (debug) 
	        			System.out.println();
	        		for (int i=1; i <= mdcolumnCount ; i++) {
	        			String colname = rsmd.getColumnName(i).toLowerCase();
	        			if (colname.equals("create_location_id")
	        					|| colname.equals("create_operator_id")
	        					|| colname.equals("freetext")
	        					|| colname.equals("historical_bookings")
	        					|| colname.equals("historical_browses")
	        					|| colname.equals("historical_charges")
	        					|| colname.equals("magnetic_media")
	        					|| colname.equals("media_type_id")
	        					|| colname.equals("modify_location_id")
	        					|| colname.equals("modify_operator_id")
	        					|| colname.equals("pieces")
	        					|| colname.equals("price")
	        					|| colname.equals("reserve_charges")
	        					|| colname.equals("sensitize")
	        					|| colname.equals("short_loan_charges")
	        					|| colname.equals("spine_label"))
	        				continue;
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
	       					Location l = locations.getByNumber(Integer.valueOf(value));
	       					if (l != null) {
		       					if (colname.equals("perm_location"))
		       						loc = l.code;
		       					else if ( l.library != null )
		       						tempLibrary = l.library;
	       						record.put(colname, l);
	       					} else record.put(colname, value);
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
		        			enumeration.replaceAll("Bound with", "");
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
	        		items.put(Integer.valueOf((String)record.get("item_id")),record);
	        		foundItems = true;
	        		if (tempLibrary != null)
	        			workLibraries.add(tempLibrary);
	        		else if (holdingsLibrary != null)
	        			workLibraries.add(holdingsLibrary);
	        	}
	        } catch (SQLException ex) {
	           System.out.println(query);
	           System.out.println(ex.getMessage());
	        }
	        if ( ! foundItems && holdingsLibrary != null )
	        	workLibraries.add(holdingsLibrary);
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
			Collection<String> nonBlankEnums = new HashSet<>();
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
			Boolean descriptionLooksMultivol = doesDescriptionLookMultivol();
			if (description_with_e) {
				// this is strong evidence for case 3
				if (descriptionLooksMultivol == null || ! descriptionLooksMultivol) {
					// confirm case 3
					multivol = true;
				    SolrInputField t = new SolrInputField("mainitem_b");
				    t.setValue(true, 1.0f);
					fields.put("mainitem_b", t);
				} else {
					// multivol with an e? Not sure here, but concluding case 1
					multivol = true;
				    SolrInputField t = new SolrInputField("enumerror_b");
				    t.setValue(true, 1.0f);
				    fields.put("enumerror_b",t);
				}
			} else {
				boolean solved = false;
				// Serial title?
				if (rectypebiblvl.equals("ab") || rectypebiblvl.equals("as")) {
					//concluding case 1 for now
					multivol = true;
					SolrInputField t = new SolrInputField("enumerror_b");
					t.setValue(true, 1.0f);
					fields.put("enumerror_b",t);
					solved = true;
				}
				if (! solved && multivol) {
					// if there's no e but we have identified the work as a multivol already
					// (due to enumeration diversity), the conclude 3, but flag it.
				    SolrInputField t = new SolrInputField("enumerror_b");
				    t.setValue(true, 1.0f);
				    fields.put("enumerror_b",t);
				    t = new SolrInputField("mainitem_b");
				    t.setValue(true, 1.0f);
				    fields.put("mainitem_b",t);
				    solved = true;
				}
				if (! solved) {
					// not known to be a multivol, has no 300e, isn't a serial
					// conclude 2 for now.
				}
			}
		}

		if (blankEnum && multivol) {
			SolrInputField multivolWithBlank = new SolrInputField("multivolwblank_b");
			multivolWithBlank.setValue(true, 1.0f);
			fields.put("multivolwblank_b", multivolWithBlank);
			for (Map<String,Object> record : items.values()) {
				String enumeration = record.get("item_enum").toString() + 
    					record.get("chron") + record.get("year");
				if (enumeration.isEmpty()) {
					Holdings h = holdings.get(record.get("mfhd_id").toString());
					record.put("item_enum", StringUtils.join(",  ",h.holdings_desc));
				}
			}
		}

		multivolField.setValue(multivol, 1.0f);
		fields.put("multivol_b", multivolField);
		SolrInputField itemField = new SolrInputField("item_record_display");
		SolrInputField itemlist = new SolrInputField("item_display");

		for (Map<String,Object> record : items.values()) {
	 		String json = mapper.writeValueAsString(record);
			if (debug)
				System.out.println(json);
			itemField.addValue(json, 1);
			StringBuilder item = new StringBuilder();
			item.append(record.get("item_id"));
			String moddate = record.get("modify_date").toString();
			item.append('|');
			item.append(record.get("mfhd_id"));
			if ( ! moddate.isEmpty()) {
	    		item.append('|');
				item.append(moddate.replaceAll("[^0-9]", "").substring(0, 14));
			}
			itemlist.addValue(item.toString(),1);
		}

		fields.put("item_display", itemlist);
		fields.put("item_record_display", itemField);
	}

	private static Pattern multivolDesc = null;
	private static Pattern singlevolDesc = null;
	private Boolean doesDescriptionLookMultivol() {
		if (descriptions.isEmpty()) return null;

		for (String desc : descriptions) {
			if (multivolDesc == null) multivolDesc = Pattern.compile("(\\d+)\\D* v\\.");
			Matcher m = multivolDesc.matcher(desc);
			if (m.find()) {
				int c = Integer.valueOf(m.group(1).replaceAll("[^\\d\\-\\.]", ""));
				if (c > 1) return true;
				if (c == 1) return false;
			}
			if (singlevolDesc == null) singlevolDesc = Pattern.compile("^([^0-9\\-\\[\\]  ] )?p\\.");
			if (singlevolDesc.matcher(desc).find()) return false;
		}

		return null;
	}

    private static String convertClobToString(Clob clob) throws Exception {
        StringWriter writer = new StringWriter();
        try (  InputStream inputStream = clob.getAsciiStream() ) {
        	IOUtils.copy(inputStream, writer, "utf-8"); }
        return writer.toString();
     }

	private static class LocationEnumStats {
		public String aFoundEnum = null;
		public Boolean blankEnumFound = false;
		public Boolean diverseEnumFound = false;
	}

	public static class Holdings {
		public String id;
		public String modified_date = null;
		public String copy_number = null;
		public String[] callnos;
		public String[] notes;
		public String[] holdings_desc;
		public String[] recent_holdings_desc;
		public String[] supplemental_holdings_desc;
		public String[] index_holdings_desc;
		public Location[] locations;
	}


}
