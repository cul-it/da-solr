package edu.cornell.library.integration.indexer.fieldMaker;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.RemoveTrailingPunctuation;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.Subfield;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;


/**
 * Get values for subfields List in order and put into
 * a SolrInputField  
 */
public class StandardMARCFieldMaker implements FieldMaker {
		
	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag,
			String marcSubfieldCodes, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes, VernMode vernMode, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes, VernMode vernMode){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
	}
	
	String marcSubfieldCodes = "";
	String marcFieldTag = null;
	String solrFieldName = null;
	String solrVernFieldName = null;
	String unwantedChars = null;
	VernMode vernMode = VernMode.ADAPTIVE;

	public String getName() {
		return SubfieldCodeMaker.class.getSimpleName() +
				" for MARC field " + marcFieldTag + 
				" and codes " + marcSubfieldCodes;
	}
	
	private String calcVernFieldName( String fieldName ) {
		if (fieldName.lastIndexOf('_') >= 0) {
			return fieldName.substring(0, fieldName.lastIndexOf('_')) + "_vern" + fieldName.substring(fieldName.lastIndexOf('_'));
		} else {
			return fieldName + "_vern";
		}
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, RDFService mainStore,
			RDFService localStore) throws Exception {
		//need to setup query once the recordURI is known
		//subfield values filtered to only the ones requested
		String query = 
				"SELECT (str(?f) as ?field) (str(?sf) as ?sfield) ?tag ?code ?value WHERE { \n"+
				"<"+recordURI+"> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField" + marcFieldTag + "> ?f . \n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> ?sf .\n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> ?tag .\n"+
				"?sf <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?value .\n"+
				"?sf <http://marcrdf.library.cornell.edu/canonical/0.1/code> ?code\n"+
				"FILTER( CONTAINS( \"6" + marcSubfieldCodes + "\" , ?code) )\n"+
				"} ";
										
		SPARQLFieldMakerImpl impl = new SPARQLFieldMakerImpl()
			.addMainStoreQuery(queryKey, query)			
			.addResultSetToFields(new SubfieldsRStoFields());
		
		return impl.buildFields(recordURI, mainStore, localStore);		
	}

	private class SubfieldsRStoFields implements ResultSetToFields{

		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) throws Exception {
			if( results == null || results.get(queryKey) == null )
				throw new Exception( getName() + " did not get any result sets");
				
			ResultSet rs = results.get(queryKey);
			MarcRecord rec = new MarcRecord();
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				String f_uri = nodeToString( sol.get("field") );
				Integer field_no = Integer.valueOf( f_uri.substring( f_uri.lastIndexOf('_') + 1 ) );
				String sf_uri = nodeToString( sol.get("sfield") );
				Integer sfield_no = Integer.valueOf( sf_uri.substring( sf_uri.lastIndexOf('_') + 1 ) );
				DataField f;
				if (rec.data_fields.containsKey(field_no)) {
					f = rec.data_fields.get(field_no);
				} else {
					f = new DataField();
					f.id = field_no;
					f.tag = nodeToString( sol.get("tag"));
				}
				Subfield sf = new Subfield();
				sf.id = sfield_no;
				sf.code = nodeToString( sol.get("code")).charAt(0);
				sf.value = nodeToString( sol.get("value"));
				if (sf.code.equals('6')) {
					if ((sf.value.length() >= 6) && Character.isDigit(sf.value.charAt(4))
							&& Character.isDigit(sf.value.charAt(5))) {
						f.linkOccurrenceNumber = Integer.valueOf(sf.value.substring(4, 6));
					}
				}
				f.subfields.put(sfield_no, sf);
				rec.data_fields.put(field_no, f);
				
			}
			
			// Put all fields with link occurrence numbers into matchedFields to be grouped by
			// their occurrence numbers. Everything else goes in sorted fields keyed by field id
			// to be displayed in field id order.
			Map<Integer,FieldSet> matchedFields  = new HashMap<Integer,FieldSet>();
			Map<Integer,FieldSet> sortedFields = new HashMap<Integer,FieldSet>();
			Integer[] ids = rec.data_fields.keySet().toArray(new Integer[ rec.data_fields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				DataField f = rec.data_fields.get(id);
				if ((f.linkOccurrenceNumber != null) && (f.linkOccurrenceNumber != 0)) {
					FieldSet fs;
					if (matchedFields.containsKey(f.linkOccurrenceNumber)) {
						fs = matchedFields.get(f.linkOccurrenceNumber);
						if (fs.minFieldNo > f.id) fs.minFieldNo = f.id;
					} else {
						fs = new FieldSet();
						fs.linkOccurrenceNumber = f.linkOccurrenceNumber;
						fs.minFieldNo = f.id;
					}
					fs.fields.add(f);
					matchedFields.put(fs.linkOccurrenceNumber, fs);
				} else {
					FieldSet fs = new FieldSet();
					fs.minFieldNo = f.id;
					fs.fields.add(f);
					sortedFields.put(f.id, fs);
				}
			}
			// Take groups linked by occurrence number, and add them as groups to the sorted fields
			// keyed by the smallest field id of the group. Groups will be added together, but with
			// that highest precendence of the lowest field id.
			for( Integer linkOccurrenceNumber : matchedFields.keySet() ) {
				FieldSet fs = matchedFields.get(linkOccurrenceNumber);
				sortedFields.put(fs.minFieldNo, fs);
			}
			
			if (sortedFields.keySet().size() == 0)
				return Collections.emptyMap();

			Map<String,SolrInputField> fieldmap = new HashMap<String,SolrInputField>();
			
			SolrInputField solrField = new SolrInputField(solrFieldName);
			fieldmap.put(solrFieldName, solrField);
			if (vernMode.equals(VernMode.VERNACULAR)) {
				solrField = new SolrInputField(solrVernFieldName);
				fieldmap.put(solrVernFieldName,solrField);
			}

			// For each field and/of field group, add to SolrInputFields in precedence (field id) order,
			// but with organization determined by vernMode.
			ids = sortedFields.keySet().toArray( new Integer[ sortedFields.keySet().size() ]);
			Arrays.sort( ids );
			for( Integer id: ids) {
				FieldSet fs = sortedFields.get(id);
				DataField[] fields = fs.fields.toArray( new DataField[ fs.fields.size() ]);
				// If a "group" contains only one field, the organization is straightforward.
				if (fs.fields.size() == 1) {
					DataField f = fields[0];
					if (vernMode.equals(VernMode.VERNACULAR) && f.tag.equals("880")) {
						fieldmap.get(solrVernFieldName).addValue(concatenateSubfields(f).trim(), 1.0f);
					} else {
						fieldmap.get(solrFieldName).addValue(concatenateSubfields(f).trim(), 1.0f);
					}
			    // If more than one field in a group, there are several options.
				} else {
					Map<Integer,DataField> reordered = new HashMap<Integer,DataField>();
					for (DataField f: fields) {
						reordered.put(f.id, f);
					}
					Integer[] field_ids = reordered.keySet().toArray( new Integer[ reordered.keySet().size() ]);
					Arrays.sort(field_ids);
					Set<String> values880 = new HashSet<String>();
					Set<String> valuesMain = new HashSet<String>();
					for (Integer fid: field_ids) {
						DataField f = reordered.get(fid);
						String value = concatenateSubfields(f).trim();
						if (value.length() == 0) continue;
						if (f.tag.equals("880")) values880.add(value);
						else valuesMain.add(value);
					}
					Iterator<String> i880 = values880.iterator();
					Iterator<String> iMain = valuesMain.iterator();
					if (vernMode == VernMode.VERNACULAR) {
						while (i880.hasNext())
							fieldmap.get(solrVernFieldName).addValue(i880.next(), 1.0f);
						while (iMain.hasNext())
							fieldmap.get(solrFieldName).addValue(iMain.next(), 1.0f);
					} else if ((values880.size() == 1) && (valuesMain.size() == 1)) {
						String s880 = i880.next();
						String sMain = iMain.next();
						if (s880.equals(sMain)) {
							fieldmap.get(solrFieldName).addValue(sMain,1.0f);
						} else {
							if (vernMode == VernMode.COMBINED) {
								fieldmap.get(solrFieldName).addValue(s880+" / " + sMain, 1.0f);
							} else if (vernMode == VernMode.ADAPTIVE) {
								if (s880.length() <= 10) {
									fieldmap.get(solrFieldName).addValue(s880+" / " + sMain, 1.0f);
								} else {
									fieldmap.get(solrFieldName).addValue(s880, 1.0f);
									fieldmap.get(solrFieldName).addValue(sMain, 1.0f);
								}
							} else { //VernMode.SEPARATE
								fieldmap.get(solrFieldName).addValue(s880, 1.0f);
								fieldmap.get(solrFieldName).addValue(sMain, 1.0f);
							}
						}
					} else { // COMBINED and ADAPTIVE vernModes default to SEPARATE if
						     // there aren't exactly one each of 880 and "other" in fieldset
						while (i880.hasNext())
							fieldmap.get(solrFieldName).addValue(i880.next(), 1.0f);
						while (iMain.hasNext())
							fieldmap.get(solrFieldName).addValue(iMain.next(), 1.0f);
					}
				}
			}			

			return fieldmap;
			
		}		

		private String concatenateSubfields( DataField f ) {
			StringBuilder sb = new StringBuilder();
			Integer[] sf_ids = f.subfields.keySet().toArray( new Integer[ f.subfields.keySet().size() ]);
			Arrays.sort(sf_ids);
			Boolean first = true;
			for(Integer sf_id: sf_ids) {
				Subfield sf = f.subfields.get(sf_id);
				if (sf.code.equals('6')) continue;
				
				if (first) first = false;
				else sb.append(" ");
				sb.append(sf.value.trim());
			}
			if (unwantedChars != null) {
				return RemoveTrailingPunctuation(sb.toString(),unwantedChars);
			} else {
				return sb.toString();
			}
		}
	};

	private final String queryKey = "query";
	
	private static class FieldSet {
		Integer minFieldNo;
		Integer linkOccurrenceNumber;
		Set<DataField> fields = new HashSet<DataField>();
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(this.fields.size() + "fields / link occurrence number: " + 
			          this.linkOccurrenceNumber +"/ min field no: " + this.minFieldNo);
			Iterator<DataField> i = this.fields.iterator();
			while (i.hasNext()) {
				sb.append(i.next().toString() + "\n");
			}
			return sb.toString();
		}
	}
	
	public static enum VernMode {
		VERNACULAR, // non-Roman values go in vern field
		SEPARATE,   // non-Roman values go in separate entries in same field
		COMBINED,   // non-Roman values go into combined entry with Romanized values
		ADAPTIVE    // COMBINED for short values, SEPARATE for long
	}
}
