package edu.cornell.library.integration.indexer.fieldMaker;

import static edu.cornell.library.integration.utilities.CharacterSetUtils.hasCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.isCJK;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.trimInternationally;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.standardizeApostrophes;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.PDF_closeRTL;
import static edu.cornell.library.integration.utilities.CharacterSetUtils.RLE_openRTL;
import static edu.cornell.library.integration.utilities.IndexingUtilities.removeTrailingPunctuation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.DataField;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;


/**
 * Get values for subfields List in order and put into
 * a SolrInputField  
 */
public class StandardMARCFieldMaker implements FieldMaker {
		
	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag,
			IndicatorReq ir, String marcSubfieldCodes, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
		this.indicatorReq = ir;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag,
			String marcSubfieldCodes, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			IndicatorReq ir, String marcSubfieldCodes, VernMode vernMode, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR) 
				|| this.vernMode.equals(VernMode.SING_VERN))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
		this.indicatorReq = ir;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes, VernMode vernMode, String unwantedChars){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.unwantedChars = unwantedChars;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR) 
				|| this.vernMode.equals(VernMode.SING_VERN))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			IndicatorReq ir, String marcSubfieldCodes, VernMode vernMode){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR)
				|| this.vernMode.equals(VernMode.SING_VERN))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
		this.indicatorReq = ir;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes, VernMode vernMode){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR)
				|| this.vernMode.equals(VernMode.SING_VERN))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			String marcSubfieldCodes, VernMode vernMode, Boolean titleMode){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.vernMode = vernMode;
		if (this.vernMode.equals(VernMode.VERNACULAR)
				|| this.vernMode.equals(VernMode.SING_VERN))
			this.solrVernFieldName = calcVernFieldName(this.solrFieldName);
		this.titleMode = titleMode;
	}

	public StandardMARCFieldMaker(String solrFieldName, String marcFieldTag, 
			IndicatorReq ir, String marcSubfieldCodes){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
		this.indicatorReq = ir;
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
	IndicatorReq indicatorReq = null;
	VernMode vernMode = VernMode.ADAPTIVE;
	Boolean titleMode = false;

	public String getName() {
		return StandardMARCFieldMaker.class.getSimpleName() +
				" for MARC field " + marcFieldTag + 
				" and codes " + marcSubfieldCodes;
	}
	
	private static String calcVernFieldName( String fieldName ) {
		if (fieldName.lastIndexOf('_') >= 0) {
			return fieldName.substring(0, fieldName.lastIndexOf('_')) + "_vern" + fieldName.substring(fieldName.lastIndexOf('_'));
		}
		return fieldName + "_vern";
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, SolrBuildConfig config) throws Exception {
		//need to setup query once the recordURI is known
		//subfield values filtered to only the ones requested
		StringBuilder sb = new StringBuilder();
		if (indicatorReq == null) indicatorReq = new IndicatorReq();
		sb.append(
				"SELECT (str(?f) as ?field) (str(?sf) as ?sfield) ?tag ?code ?value ?ind1 ?ind2 WHERE { \n"+
				"<"+recordURI+"> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField" + marcFieldTag + "> ?f . \n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> ?sf .\n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> ?tag .\n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/ind1> ?ind1 .\n");
		if (indicatorReq.equals1 != null)
			sb.append("?f <http://marcrdf.library.cornell.edu/canonical/0.1/ind1> \"" +indicatorReq.equals1+ "\"^^<http://www.w3.org/2001/XMLSchema#string>.\n");
		sb.append("?f <http://marcrdf.library.cornell.edu/canonical/0.1/ind2> ?ind2 .\n");
		if (indicatorReq.equals2 != null)
			sb.append("?f <http://marcrdf.library.cornell.edu/canonical/0.1/ind1> \"" +indicatorReq.equals2+ "\"^^<http://www.w3.org/2001/XMLSchema#string>.\n");
		sb.append(
				"?sf <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?value .\n"+
				"?sf <http://marcrdf.library.cornell.edu/canonical/0.1/code> ?code\n");
		sb.append( "FILTER( " );
		if (indicatorReq.in1 != null)
			sb.append("      CONTAINS( \"" +indicatorReq.in1+ "\" , ?ind1) && \n");
		if (indicatorReq.in2 != null)
			sb.append("      CONTAINS( \"" +indicatorReq.in2+ "\" , ?ind2) && \n");
		sb.append( "         CONTAINS( \"6" + marcSubfieldCodes + "\" , ?code) )\n"
				+ "} ");
		SPARQLFieldMakerImpl impl = new SPARQLFieldMakerImpl()
			.addMainStoreQuery(queryKey, sb.toString())			
			.addResultSetToFields(new SubfieldsRStoFields());
		
		return impl.buildFields(recordURI, config);		
	}

	private class SubfieldsRStoFields implements ResultSetToFields{

		@Override
		public Map<String, SolrInputField> toFields(
				Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {
			if( results == null || results.get(queryKey) == null )
				throw new Exception( getName() + " did not get any result sets");
				
			MarcRecord rec = new MarcRecord();
			rec.addDataFieldResultSet(results.get(queryKey),marcFieldTag);
			
			Collection<FieldSet> sortedFields = rec.matchAndSortDataFields(vernMode);
			
			if (sortedFields.isEmpty())
				return Collections.emptyMap();

			Map<String,SolrInputField> fieldmap = new HashMap<>();
			
			SolrInputField solrField = new SolrInputField(solrFieldName);
			fieldmap.put(solrFieldName, solrField);
			if (vernMode.equals(VernMode.VERNACULAR)
					|| vernMode.equals(VernMode.SING_VERN)) {
				solrField = new SolrInputField(solrVernFieldName);
				fieldmap.put(solrVernFieldName,solrField);
			}
			if (vernMode.equals(VernMode.SEARCH)) {
				solrField = new SolrInputField(solrFieldName + "_cjk");
				fieldmap.put(solrFieldName+"_cjk", solrField);
			}

			for( FieldSet fs: sortedFields ) {

				if (fs.fields.size() == 1) {
					DataField f = fs.fields.iterator().next();
					String val = trimInternationally( concatenateSubfields(f) );
					if (val.length() == 0) continue;
					if ((vernMode.equals(VernMode.VERNACULAR)
							|| vernMode.equals(VernMode.SING_VERN))
						&& f.tag.equals("880")) {
						fieldmap.get(solrVernFieldName).addValue(val, 1.0f);
					} else if (vernMode.equals(VernMode.SEARCH)) {
						if (f.tag.equals("880")) {
							if (f.getScript().equals(MarcRecord.Script.CJK))
								fieldmap.get(solrFieldName + "_cjk").addValue(val, 1.0f);
							else {
								if (hasCJK(val))
									fieldmap.get(solrFieldName + "_cjk").addValue(val, 1.0f);
								fieldmap.get(solrFieldName).addValue(standardizeApostrophes(val), 1.0f);
								if (titleMode)
									fieldmap.get(solrFieldName).addValue(standardizeApostrophes(
											f.getStringWithoutInitialArticle(val)), 1.0f);
							}							
						} else {
							if (isCJK(val))
								fieldmap.get(solrFieldName + "_cjk").addValue(val, 1.0f);
							fieldmap.get(solrFieldName).addValue(standardizeApostrophes(val), 1.0f);
							if (titleMode)
								fieldmap.get(solrFieldName).addValue(standardizeApostrophes(
										f.getStringWithoutInitialArticle(val)), 1.0f);
						}
					} else {
						fieldmap.get(solrFieldName).addValue(val, 1.0f);
					}
			    // If more than one field in a group, there are several options.
				} else {
					Map<Integer,DataField> reordered = new TreeMap<>();
					for (DataField f: fs.fields) {
						reordered.put(f.id, f);
					}
					Set<String> values880 = new HashSet<>();
					Set<String> valuesMain = new HashSet<>();
					Set<String> valuesCJK = new HashSet<>();

					for (DataField f: reordered.values()) {
						String value = trimInternationally( concatenateSubfields(f) );
						if (value.length() == 0) continue;
						if (f.tag.equals("880")) {
							if (vernMode.equals(VernMode.SEARCH)) {
								if (f.getScript().equals(MarcRecord.Script.CJK)) {
									valuesCJK.add(value);
								} else {
									values880.add(value);
									if (hasCJK(value))
										valuesCJK.add(value);
								}
							} else {
								values880.add(value);
							}
						} else {
							if (vernMode.equals(VernMode.SEARCH))
								if (isCJK(value))
									valuesCJK.add(value);
							valuesMain.add(value);
						}
					}
					Iterator<String> i880 = values880.iterator();
					Iterator<String> iMain = valuesMain.iterator();
					if ((values880.size() == 1) && (valuesMain.size() == 1)) {
						String s880 = i880.next();
						String sMain = iMain.next();
						if (s880.equals(sMain)) {
							if (sMain.length() > 0)
								if (vernMode.equals(VernMode.SEARCH))
									fieldmap.get(solrFieldName).addValue(standardizeApostrophes(sMain),1.0f);
								else
									fieldmap.get(solrFieldName).addValue(sMain,1.0f);
						} else {
							if ((vernMode == VernMode.COMBINED) || (vernMode == VernMode.SINGULAR)) {
								fieldmap.get(solrFieldName).addValue(s880+" / " + sMain, 1.0f);
							} else if (vernMode == VernMode.ADAPTIVE) {
								if (s880.length() <= (12 + RLE_openRTL.length() + PDF_closeRTL.length())) {
									fieldmap.get(solrFieldName).addValue(s880+" / " + sMain, 1.0f);
								} else {
									fieldmap.get(solrFieldName).addValue(s880, 1.0f);
									fieldmap.get(solrFieldName).addValue(sMain, 1.0f);
								}
							} else if ((vernMode == VernMode.VERNACULAR) 
									|| (vernMode == VernMode.SING_VERN)){
								fieldmap.get(solrVernFieldName).addValue(s880, 1.0f);
								fieldmap.get(solrFieldName).addValue(sMain, 1.0f);
							} else { //VernMode.SEPARATE VernMode.SEARCH
								fieldmap.get(solrFieldName).addValue(s880, 1.0f);
								if (vernMode.equals(VernMode.SEARCH))
									fieldmap.get(solrFieldName).addValue(standardizeApostrophes(sMain),1.0f);
								else
									fieldmap.get(solrFieldName).addValue(sMain,1.0f);
							}
						}
					} else { // COMBINED and ADAPTIVE vernModes default to SEPARATE if
						     // there aren't exactly one each of 880 and "other" in fieldset
						if (vernMode != VernMode.SINGULAR) {
							while (i880.hasNext())
								if (vernMode.equals(VernMode.VERNACULAR)
										|| vernMode.equals(VernMode.SING_VERN)) {
									fieldmap.get(solrVernFieldName).addValue(i880.next(), 1.0f);
								} else {
									fieldmap.get(solrFieldName).addValue(i880.next(), 1.0f);
								}
							if (vernMode.equals(VernMode.SEARCH))
								while (iMain.hasNext())
									fieldmap.get(solrFieldName).addValue(standardizeApostrophes(iMain.next()), 1.0f);
							else
								while (iMain.hasNext())
									fieldmap.get(solrFieldName).addValue(iMain.next(), 1.0f);
						} else {
							StringBuilder sb = new StringBuilder();
							while (i880.hasNext()) {
								sb.append(" ");
								sb.append(i880.next());
							}
							while (iMain.hasNext()) {
								sb.append(" ");
								sb.append(iMain.next());
							}
							String val = trimInternationally( sb.toString() );
							if (val.length() > 0)
								fieldmap.get(solrFieldName).addValue(val, 1.0f);
						}
						if (vernMode == VernMode.SEARCH) {
							Iterator<String> i = valuesCJK.iterator();
							while (i.hasNext())
								fieldmap.get(solrFieldName + "_cjk").addValue(i.next(), 1.0f);
						}
					}
				}
			}
			if (vernMode.equals(VernMode.SINGULAR) || vernMode.equals(VernMode.SING_VERN)) {
				SolrInputField field = fieldmap.get(solrFieldName);
				if (field.getValueCount() > 1)
					fieldmap.put(solrFieldName, concatenateValues( field ));
				if (vernMode.equals(VernMode.SING_VERN)) {
					field = fieldmap.get(solrVernFieldName);
					if (field.getValueCount() > 1)
						fieldmap.put(solrVernFieldName, concatenateValues( field ));
				}
			}
			
			Map<String,SolrInputField> populatedFields = new HashMap<>();
			for(String fieldName: fieldmap.keySet()) {
				if (fieldmap.get(fieldName).getValueCount() > 0)
					populatedFields.put(fieldName, fieldmap.get(fieldName));
			}
			return fieldmap;
			
		}
		
		private SolrInputField concatenateValues( SolrInputField field ) {
			Iterator<Object> i = field.getValues().iterator();
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			while (i.hasNext()) {
				if (first) first = false;
				else sb.append(' ');
				sb.append(i.next().toString());
			}
			SolrInputField newField = new SolrInputField(field.getName());
			newField.setValue(sb.toString(), 1.0f);			
			return newField;
		}

		private String concatenateSubfields( DataField f ) {
			String value = f.concatenateSubfieldsOtherThan("6");
			if (unwantedChars != null) {
				return removeTrailingPunctuation(value,unwantedChars);
			}
			return value;
		}
	}

	private final String queryKey = "query";
	
	
	public static enum VernMode {
		VERNACULAR, // non-Roman values go in vern field
		SEPARATE,   // non-Roman values go in separate entries in same field
		COMBINED,   // non-Roman values go into combined entry with Romanized values
		ADAPTIVE,   // COMBINED for short values, SEPARATE for long
		
		SINGULAR,  // same as COMBINED, but combines all values, without regard to link_id.
		
		SING_VERN,  // same as VERNACULAR, $6 occurrence numbers are disregarded and all
		           // values are treated as matching
		SEARCH     // CJK values get "_cjk" postpended to their field names, indexes as SEPARATE
	}
}
