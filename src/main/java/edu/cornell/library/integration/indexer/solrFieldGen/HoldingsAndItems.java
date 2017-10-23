package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.utilities.IndexingUtilities.insertSpaceAfterCommas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.BooleanSolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrField;
import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.SolrFields;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.DataFieldSet;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.Locations;
import edu.cornell.library.integration.voyager.Locations.Location;

/**
 * Collecting holdings data needed by the Blacklight availability service into holdings_record_display.
 * For a bib record with multiple holdings records, each holdings record will have it's own holdings_record_display.
 */
public class HoldingsAndItems implements ResultSetToFields {

	private static Boolean debug = false;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		Map<String,MarcRecord> holdingRecs = new HashMap<>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();


				if ( resultKey.equals("description")) {

					JenaResultsToMarcRecord.addDataFieldQuerySolution(bibRec,sol);

				} else if ( resultKey.equals("leader") ) {

					bibRec.leader = nodeToString(sol.get("leader"));

				} else {
	 				String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (holdingRecs.containsKey(recordURI)) {
						rec = holdingRecs.get(recordURI);
					} else {
						rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
						holdingRecs.put(recordURI, rec);
					}
					if (resultKey.contains("control")) {
						JenaResultsToMarcRecord.addControlFieldQuerySolution(rec,sol);
					} else if (resultKey.contains("data")) {
						JenaResultsToMarcRecord.addDataFieldQuerySolution(rec,sol);
					}

				}
			}
		}
		bibRec.holdings.addAll(holdingRecs.values());
		SolrFields vals = generateSolrFields( bibRec, config );
		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		for ( BooleanSolrField f : vals.boolFields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	public static SolrFields generateSolrFields( MarcRecord bibRec, SolrBuildConfig config )
			throws ClassNotFoundException, SQLException, IOException {
		Collection<String> descriptions = new HashSet<>();
		boolean description_with_e = false;
		String rectypebiblvl = null;
		Locations locations = new Locations(config);
		Collection<String> holding_ids = new TreeSet<>();
		Map<String,Holdings> holdings = new TreeMap<>();
		Collection<Map<String,Object>> boundWiths = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		Collection<Location> workLocations = new LinkedHashSet<>();



		for (DataField f : bibRec.dataFields) {
			descriptions.add(f.concatenateSubfieldsOtherThan6());
			for (Subfield sf : f.subfields)
				if (sf.code.equals('e'))
					description_with_e = true;
		}
		rectypebiblvl = bibRec.leader.substring(6,8);

		SolrFields sfs = new SolrFields();
		for( MarcRecord rec: bibRec.holdings ) {

			Collection<Location> holdingLocations = new HashSet<>();
			Collection<String> callnos = new HashSet<>();
			List<String> holdingDescs = new ArrayList<>();
			List<String> recentHoldings = new ArrayList<>();
			List<String> supplementalHoldings = new ArrayList<>();
			List<String> indexHoldings = new ArrayList<>();
			List<String> notes = new ArrayList<>();
			String copyNo = null;

			Collection<DataFieldSet> sortedFields = rec.matchAndSortDataFields();

			for( DataFieldSet fs: sortedFields ) {

				for (DataField f: fs.getFields()) {
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
						for (Subfield sf: f.subfields) {
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
						registerBoundWith(config, rec.id, f, boundWiths);
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
			if (rec.modifiedDate != null) {
				if (rec.modifiedDate.length() >= 14)
					holding.modified_date = rec.modifiedDate.substring(0, 14);
				else
					holding.modified_date = rec.modifiedDate;
			}

			if (holding.modified_date != null)
				sfs.add(new SolrField("holdings_display",holding.id+"|"+holding.modified_date));
			else
				sfs.add(new SolrField("holdings_display",holding.id));
			ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream,holding);
			String json = jsonstream.toString("UTF-8");
			sfs.add(new SolrField("holdings_record_display",json));
			holdings.put(holding.id, holding);
			holding_ids.add(holding.id);

		}
		if (debug) System.out.println("holdings found: "+StringUtils.join(", ", holding_ids));

		// ITEM DATA
		Integer emptyItemCount[] = {0};
		try (Connection conn = config.getDatabaseConnection("Voy")){
			sfs.addAll( loadItemData(conn, holdings, locations, workLocations,
					description_with_e, rectypebiblvl, descriptions, emptyItemCount) );
		}

		if (! boundWiths.isEmpty()) {
			boolean suppressBoundWiths = (boundWiths.size() == emptyItemCount[0]);
			for (Map<String,Object> boundWith : boundWiths) {
				sfs.add(new SolrField("barcode_addl_t",boundWith.get("barcode").toString()));
				if (suppressBoundWiths)
					continue;
				ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
				mapper.writeValue(jsonstream, boundWith);
				String json = jsonstream.toString("UTF-8");
				sfs.add(new SolrField("bound_with_json",json));
			}
			sfs.add(new BooleanSolrField("suppress_bound_with_b",suppressBoundWiths));
			if ( ! suppressBoundWiths && emptyItemCount[0] > 0 )
				sfs.add(new BooleanSolrField("bound_with_count_empty_item_mismatch_b",true));
		}

		boolean isAtTheLibrary = false;
		for (Location lib : workLocations)
			if (lib.library != null) {
				sfs.add(new SolrField("location_facet",lib.library));
				sfs.add(new SolrField("location",lib.library));
				sfs.add(new SolrField("location",lib.library+" > "+lib.name));
				isAtTheLibrary = true;
			}
		if ( isAtTheLibrary )
			sfs.add(new SolrField("online","At the Library"));

		return sfs;
	}

	private static void registerBoundWith(
			SolrBuildConfig config, String mfhd_id, DataField f, Collection<Map<String,Object>> boundWiths)
					throws ClassNotFoundException, SQLException {
		String item_enum = "";
		String barcode = null;
		for (Subfield sf : f.subfields) {
			switch (sf.code) {
			case 'p': barcode = sf.value; break;
			case '3': item_enum = sf.value; break;
			}
		}
		if (barcode == null) return;

		// lookup item id here!!!
		int item_id = 0;
		try (  Connection conn = config.getDatabaseConnection("Voy");
				Statement stmt = conn.createStatement() ){
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

	private static SolrFields loadItemData(Connection conn, Map<String,Holdings> holdings,
			Locations locations, Collection<Location> workLocations, boolean description_with_e,
			String rectypebiblvl, Collection<String> descriptions, Integer[] emptyItemCount)
					throws IOException {

		SolrFields sfs = new SolrFields();
		Boolean multivol = false;

		if (holdings.isEmpty()) {
			sfs.add( new BooleanSolrField("multivol_b", false));
			return sfs;
		}
		
		Map<String,LocationEnumStats> enumStats = new HashMap<>();
		Map<Integer,Map<String,Object>> items = new TreeMap<>();
		ObjectMapper mapper = new ObjectMapper();
		for (Holdings h : holdings.values()) {

			Location holdingsLibrary = null;
			for (int i = 0; i < h.locations.length; i++)
				holdingsLibrary = h.locations[i];

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
	        		Location tempLibrary = null;
	        		String barcode = "";
	        		
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
	       				if (colname.equals("item_barcode"))
	       					barcode = value;
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
		       						tempLibrary = l;
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
	        		if (barcode.isEmpty())
	        			emptyItemCount[0]++;
	        		foundItems = true;
	        		if (tempLibrary != null)
	        			workLocations.add(tempLibrary);
	        		else if (holdingsLibrary != null)
	        			workLocations.add(holdingsLibrary);
	        	}
	        } catch (SQLException ex) {
	           System.out.println(query);
	           System.out.println(ex.getMessage());
	        }
	        if ( ! foundItems && holdingsLibrary != null )
	        	workLocations.add(holdingsLibrary);
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
			Boolean descriptionLooksMultivol = doesDescriptionLookMultivol(descriptions);
			if (description_with_e) {
				// this is strong evidence for case 3
				if (descriptionLooksMultivol == null || ! descriptionLooksMultivol) {
					// confirm case 3
					multivol = true;
					sfs.add( new BooleanSolrField("mainitem_b", true) );
				} else {
					// multivol with an e? Not sure here, but concluding case 1
					multivol = true;
					sfs.add( new BooleanSolrField("enumerror_b",true));
				}
			} else {
				boolean solved = false;
				// Serial title?
				if (rectypebiblvl.equals("ab") || rectypebiblvl.equals("as")) {
					//concluding case 1 for now
					multivol = true;
					sfs.add( new BooleanSolrField("enumerror_b",true) );
					solved = true;
				}
				if (! solved && multivol) {
					// if there's no e but we have identified the work as a multivol already
					// (due to enumeration diversity), the conclude 3, but flag it.
					sfs.add( new BooleanSolrField("enumerror_b",true));
					sfs.add( new BooleanSolrField("mainitem_b",true));
				    solved = true;
				}
				if (! solved) {
					// not known to be a multivol, has no 300e, isn't a serial
					// conclude 2 for now.
				}
			}
		}

		if (blankEnum && multivol) {
			sfs.add( new BooleanSolrField("multivolwblank_b", true));
			for (Map<String,Object> record : items.values()) {
				String enumeration = record.get("item_enum").toString() + 
    					record.get("chron") + record.get("year");
				if (enumeration.isEmpty()) {
					Holdings h = holdings.get(record.get("mfhd_id").toString());
					record.put("item_enum", StringUtils.join(",  ",h.holdings_desc));
				}
			}
		}

		sfs.add( new BooleanSolrField("multivol_b", multivol) );
		
//		SolrInputField itemField = new SolrInputField("item_record_display");
//		SolrInputField itemlist = new SolrInputField("item_display");

		for (Map<String,Object> record : items.values()) {
	 		String json = mapper.writeValueAsString(record);
			if (debug)
				System.out.println(json);
			sfs.add(new SolrField( "item_record_display",json) );
			StringBuilder item = new StringBuilder();
			item.append(record.get("item_id"));
			String moddate = record.get("modify_date").toString();
			item.append('|');
			item.append(record.get("mfhd_id"));
			if ( ! moddate.isEmpty()) {
	    		item.append('|');
				item.append(moddate.replaceAll("[^0-9]", "").substring(0, 14));
			}
			sfs.add(new SolrField( "item_display",item.toString()) );
		}
		return sfs;
	}

	private static Pattern multivolDesc = null;
	private static Pattern singlevolDesc = null;
	private static Boolean doesDescriptionLookMultivol(Collection<String> descriptions) {
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

    private static String convertClobToString(Clob clob) throws IOException, SQLException {
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
