package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.insertSpaceAfterCommas;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.QuerySolution;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.fieldMaker.SPARQLFieldMakerImpl;

/**
 * Collecting holdings data needed by the Blacklight availability service into holdings_record_display.
 * For a bib record with multiple holdings records, each holdings record will have it's own holdings_record_display.
 */
public class HoldingsAndItemsRSTF implements ResultSetToFields {

	private Boolean debug = true;
	static final Pattern p = Pattern.compile(".*_([0-9]+)");
	LocationSet locations = new LocationSet();
	Collection<String> holding_ids = new HashSet<String>();
	Map<String,Holdings> holdings = new HashMap<String,Holdings>();
	Map<String,SolrInputField> fields = new HashMap<String,SolrInputField>();
	Collection<String> descriptions = new HashSet<String>();
	boolean description_with_e = false;
	String rectypebiblvl = null;
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, com.hp.hpl.jena.query.ResultSet> results, SolrBuildConfig config) throws Exception {
		
		Map<String,MarcRecord> recs = new HashMap<String,MarcRecord>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();

				if (resultKey.equals("location")) {
					if (debug) System.out.println("**"+sol.toString());
					Location l = new Location();
					l.code = nodeToString(sol.get("code"));
					l.name = nodeToString(sol.get("name"));
					l.library = nodeToString(sol.get("library"));
					Matcher m = p.matcher(nodeToString(sol.get("locuri")));
					if (m.matches()) {
						l.number = Integer.valueOf(m.group(1));
					} else {
						System.out.println(nodeToString(sol.get("locuri")));
					}
					locations.add(l);
					
				} else if ( resultKey.equals("description")) {
					
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
		
		Boolean callNumSortSupplied = false;
		for( String holdingURI: recs.keySet() ) {
			MarcRecord rec = recs.get(holdingURI);
						
			for (ControlField f: rec.control_fields.values()) {
				if (f.tag.equals("001")) {
					rec.id = f.value;
				}
			}

			Collection<String> loccodes = new HashSet<String>();
			Collection<String> callnos = new HashSet<String>();
			List<String> holdings = new ArrayList<String>();
			List<String> recentHoldings = new ArrayList<String>();
			List<String> supplementalHoldings = new ArrayList<String>();
			List<String> indexHoldings = new ArrayList<String>();
			List<String> notes = new ArrayList<String>();
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
								loccodes.add(sf.value); break CODE;
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
							holdings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					case "867":
						supplementalHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					case "868":
						indexHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						break;
					}
					if (callno != null)
						callnos.add(callno);
				}
			}
			
			Holdings holding = new Holdings();
			holding.id = rec.id;
			holding.copy_number = copyNo;
			if ( ! loccodes.contains("serv,remo")) {
				holding.callnos = callnos.toArray(new String[ callnos.size() ]);
				if (! callNumSortSupplied && callnos.size() > 0) {
					addField(fields,"callnum_sort",callnos.iterator().next());
					callNumSortSupplied = true;
				}
			}
			holding.notes = notes.toArray(new String[ notes.size() ]);
			holding.holdings_desc = holdings.toArray(new String[ holdings.size() ]);
			holding.recent_holdings_desc = recentHoldings.toArray(new String[ recentHoldings.size() ]);
			holding.supplemental_holdings_desc = supplementalHoldings.toArray(new String[ supplementalHoldings.size() ]);
			holding.index_holdings_desc = indexHoldings.toArray(new String[ indexHoldings.size() ]);
			holding.locations = new Location[loccodes.size()];
			Iterator<String> iter = loccodes.iterator();
			int i = 0;
			while (iter.hasNext()) {
				String loccode = iter.next();
				holding.locations[i] = locations.getByCode(loccode);
				i++;
			}
			ObjectMapper mapper = new ObjectMapper();
			ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream, holding);
			String json = jsonstream.toString("UTF-8");
			addField(fields,"holdings_record_display",json);
			holding_ids.add(holding.id);
			

		}

		
		// ITEM DATA STARTS HERE
		Connection conn = null;
		try {
			conn = config.getDatabaseConnection("Voy");
			loadItemData(conn,config);
		} finally {
			if (conn != null) conn.close();
		}

		Boolean multivol = false;
		SolrInputField multivolField = new SolrInputField("multivol_b");

		if (holding_ids.isEmpty()) {
			multivolField.setValue(multivol, 1.0f);
			fields.put("multivol_b", multivolField);
			return fields;
		}		

		return fields;
	}
	
	private void loadItemData(Connection conn, SolrBuildConfig config) throws Exception {
		
		Boolean multivol = false;
		SolrInputField multivolField = new SolrInputField("multivol_b");

		if (holding_ids.isEmpty()) {
			multivolField.setValue(multivol, 1.0f);
			fields.put("multivol_b", multivolField);
			return;
		}
		
		Map<String,LocationEnumStats> enumStats = new HashMap<String,LocationEnumStats>();
		Collection<Map<String,Object>> items = new HashSet<Map<String,Object>>();
		ObjectMapper mapper = new ObjectMapper();
		for (Holdings h : holdings.values()) {
			
			if (debug)
				System.out.println(h.id);
			String query = 
					"SELECT CORNELLDB.MFHD_ITEM.*, CORNELLDB.ITEM.*, CORNELLDB.ITEM_TYPE.ITEM_TYPE_NAME, CORNELLDB.ITEM_BARCODE.ITEM_BARCODE " +
					" FROM CORNELLDB.MFHD_ITEM, CORNELLDB.ITEM_TYPE, CORNELLDB.ITEM" +
					" LEFT OUTER JOIN CORNELLDB.ITEM_BARCODE ON CORNELLDB.ITEM_BARCODE.ITEM_ID = CORNELLDB.ITEM.ITEM_ID " +
					" WHERE CORNELLDB.MFHD_ITEM.MFHD_ID = \'" + h.id + "\'" +
					   " AND CORNELLDB.MFHD_ITEM.ITEM_ID = CORNELLDB.ITEM.ITEM_ID" +
					   " AND CORNELLDB.ITEM.ITEM_TYPE_ID = CORNELLDB.ITEM_TYPE.ITEM_TYPE_ID" +
					   " AND CORNELLDB.ITEM_BARCODE.BARCODE_STATUS = '1'";
			if (debug)
				System.out.println(query);

	        Statement stmt = null;
	        java.sql.ResultSet rs = null;
	        ResultSetMetaData rsmd = null;
	        try {
	        	stmt = conn.createStatement();
	
	        	rs = stmt.executeQuery(query);
	        	rsmd = rs.getMetaData();
	        	int mdcolumnCount = rsmd.getColumnCount();
	        	while (rs.next()) {
		        	   
	        		Map<String,Object> record = new HashMap<String,Object>();
	        		record.put("mfhd_id",h.id);
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
	       					Location l = getLocation(config,Integer.valueOf(value));
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
	        		items.add(record);
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
			for (Map<String,Object> record : items) {
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
		
		
		for (Map<String,Object> record : items) {
	 		String json = mapper.writeValueAsString(record);
			if (debug)
				System.out.println(json);
			itemField.addValue(json, 1);
			StringBuilder item = new StringBuilder();
			item.append(record.get("item_id"));
			String moddate = record.get("modify_date").toString();
			if ( ! moddate.isEmpty()) {
	    		item.append('|');
				item.append(moddate.substring(0, 10));
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

	private Location getLocation (SolrBuildConfig config, Integer id) throws Exception{

		Location l = locations.getByNumber(id);
		if (l != null) return l;
		
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
				fm.buildFields("", config);
		if (debug) {
			System.out.println(tempfields.toString());
			System.out.println(tempfields.get("name").getValue());
		}
		l = new Location();
		l.code = tempfields.get("code").getValue().toString();
		l.name = tempfields.get("name").getValue().toString();
		if (tempfields.containsKey("library"))
			l.library = tempfields.get("library").getValue().toString();
		else
			System.out.println("No location found for loc code "+l.code+".");
		l.number = id;
		locations.add(l);
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
	
	public static class LocationSet {
		private Map<String,Location> _byCode = new HashMap<String,Location>();
		private Map<Integer,Location> _byNumber = new HashMap<Integer,Location>();
		
		public void add(Location l) {
			_byCode.put(l.code, l);
			_byNumber.put(l.number, l);
		}
		public Location getByCode( String code ) {
			if (_byCode.containsKey(code))
				return _byCode.get(code);
			return null;
		}
		public Location getByNumber( int number ) {
			if (_byNumber.containsKey(number))
				return _byNumber.get(number);
			return null;
		}
	}
	
	public static class Holdings {
		public String id;
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
