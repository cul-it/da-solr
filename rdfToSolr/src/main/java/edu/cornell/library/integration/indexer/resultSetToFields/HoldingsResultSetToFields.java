package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.addField;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.insertSpaceAfterCommas;

import java.io.ByteArrayOutputStream;
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

import org.apache.solr.common.SolrInputField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.ControlField;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;

/**
 * Collecting holdings data needed by the Blacklight availability service into holdings_record_display.
 * For a bib record with multiple holdings records, each holdings record will have it's own holdings_record_display.
 */
public class HoldingsResultSetToFields implements ResultSetToFields {

	private Boolean debug = true;
	
	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {

		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		Map<String,MarcRecord> recs = new HashMap<String,MarcRecord>();
		
		Map<String,Location> locations = new HashMap<String,Location>();
		
		Pattern p = Pattern.compile(".*_([0-9]+)");

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				
				if (resultKey.equals("location")) {
					if (debug) System.out.println("**"+sol.toString());
/*					StringBuilder sb = new StringBuilder();
					Iterator<String> i = sol.varNames();
					while (i.hasNext()) {
						String f = i.next();
						sb.append(f);
						sb.append(" ");
						sb.append(nodeToString(sol.get(f)));
						sb.append("; ");
					}
					System.out.println(sb.toString());*/
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
					locations.put(l.code, l);					
					
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
			
			Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
			
			Integer[] ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				FieldSet fs = sortedFields.get(id);
				DataField[] dataFields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
				for (DataField f: dataFields) {
					String callno = null;
					if (f.tag.equals("506")) {
						// restrictions on access note
						notes.add(f.concatenateSpecificSubfields("ancdefu3"));
					} else if (f.tag.equals("561")) {
						// ownership and custodial history
						if (! f.ind1.equals('0')) // '0' = private
							notes.add(f.concatenateSpecificSubfields("au3"));
					} else if (f.tag.equals("562")) {
						notes.add(f.concatenateSpecificSubfields("abcde3"));
						// copy and version identification note
					} else if (f.tag.equals("843")) {
						// reproduction note
						notes.add(f.concatenateSpecificSubfields("abcdefmn3"));
					} else if (f.tag.equals("845")) {
						// terms governing use and reproduction note
						notes.add(f.concatenateSpecificSubfields("abcdu3"));
					} else if (f.tag.equals("852")) {
						for (Subfield sf: f.subfields.values()) {
							if (sf.code.equals('b')) {
								loccodes.add(sf.value);
							} else if (sf.code.equals('h')) {
								// If there is a subfield ‡h, then there is a call number. So we will record
								// a concatenation of all the call number fields. If there are (erroneously)
								// multiple subfield ‡h entries in one field, the callno will be overwritten
								// and not duplicated in the call number array.
								callno = f.concatenateSpecificSubfields("hijklm");
							} else if (sf.code.equals('z')) {
								notes.add(sf.value);
							}		
						}
					} else if (f.tag.equals("866")) {
						if (f.ind1.equals(' ') && f.ind2.equals(' '))
							recentHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
						else
							holdings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
					} else if (f.tag.equals("867")) {
						supplementalHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
					} else if (f.tag.equals("868")) {
						indexHoldings.add(insertSpaceAfterCommas(f.concatenateSpecificSubfields("az")));
					}
					if (callno != null)
						callnos.add(callno);
				}
			}
			
			Holdings holding = new Holdings();
			holding.id = rec.id;
			holding.callnos = callnos.toArray(new String[ callnos.size() ]);
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
				holding.locations[i] = locations.get(loccode);
				i++;
			}
			ObjectMapper mapper = new ObjectMapper();
			ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
			mapper.writeValue(jsonstream, holding);
			String json = jsonstream.toString("UTF-8");
			addField(solrFields,"holdings_record_display",json);
		}
		
		

		
		return solrFields;
	}

	public static class Location {
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
	
	public static class Holdings {
		public String id;
		public String[] callnos;
		public String[] notes;
		public String[] holdings_desc;
		public String[] recent_holdings_desc;
		public String[] supplemental_holdings_desc;
		public String[] index_holdings_desc;
		public Location[] locations;
	}


}
