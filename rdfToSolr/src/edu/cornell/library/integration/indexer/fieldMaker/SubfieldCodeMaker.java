package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;

import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;
import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.nodeToString;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * Get values for subfields List in order and put into
 * a SolrInputField  
 */
public class SubfieldCodeMaker implements FieldMaker {
	
	
	public SubfieldCodeMaker(String solrFieldName, String marcFieldTag, String marcSubfieldCodes){ 			
		super(); 		
		this.marcSubfieldCodes = marcSubfieldCodes;
		this.marcFieldTag = marcFieldTag;
		this.solrFieldName = solrFieldName;
	}

	String marcSubfieldCodes = "";
	String marcFieldTag = null;
	String solrFieldName = null;		

	public String getName() {
		return SubfieldCodeMaker.class.getSimpleName() +
				" for MARC field " + marcFieldTag + 
				" and codes " + marcSubfieldCodes;
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, RDFService mainStore,
			RDFService localStore) throws Exception {
		//need to setup query once the recordURI is known
		//subfield values filtered to only the ones requested
		String query = 
				"SELECT (str(?f) as ?field) ?code ?value WHERE { \n"+
				"<"+recordURI+"> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?f . \n"+
			    "?f <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"" + marcFieldTag + "\" . \n"+
				"?f <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> ?sfield .\n"+
				"?sfield <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?value .\n"+
				"?sfield <http://marcrdf.library.cornell.edu/canonical/0.1/code> ?code\n"+
				"FILTER( CONTAINS( \"" + marcSubfieldCodes + "\" , ?code) )\n"+
				"} ";
										
		SPARQLFieldMakerImpl impl = new SPARQLFieldMakerImpl();
		impl.setMainStoreQueries(Collections.singletonMap(queryKey, query));
		
		List<ResultSetToFields> rs2fs = new ArrayList<ResultSetToFields>();
		rs2fs.add( new SubfieldsRStoFields());
		impl.setResultSetToFieldsList(rs2fs);
		
		return impl.buildFields(recordURI, mainStore, localStore);		
	}

	private class SubfieldsRStoFields implements ResultSetToFields{

		@Override
		public Map<? extends String, ? extends SolrInputField> toFields(
				Map<String, ResultSet> results) throws Exception {
			if( results == null || results.get(queryKey) == null )
				throw new Exception( getName() + " did not get any result sets");
				
			Map<String,Map<String,String>> codeMap = new HashMap<String, Map<String, String>>();
			ResultSet rs = results.get(queryKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				addSolToMap( codeMap, sol );				
			}

			SolrInputField solrField = new SolrInputField(solrFieldName);			

			for (String field : codeMap.keySet()) {

				String sortedVals = "";	
				Map<String,String> fieldMap = codeMap.get(field);
				for( char code : marcSubfieldCodes.toCharArray()){
					String values = fieldMap.get(Character.toString( code ));
					if( values != null ) {
						if (sortedVals.equals("")) {
							sortedVals = values;
						} else {
							sortedVals = sortedVals + " " + values;
						}
					}
				}
				if( sortedVals.trim().length() != 0){
					solrField.addValue(sortedVals, 1.0f);
				}				
			}

			if (solrField.getValueCount() > 0) {
				return Collections.singletonMap(solrFieldName, solrField);
			} else { 
				return Collections.emptyMap();
			}
		}		

		private void addSolToMap(Map<String,Map<String,String>> codeMap,
								 QuerySolution sol){
			Literal codeLit = sol.getLiteral("code");
			Literal fieldLit = sol.getLiteral("field");
			Literal valueLit = sol.getLiteral("value");
			if( codeLit != null || valueLit != null ){
				String field = nodeToString( fieldLit );
				String code = nodeToString( codeLit );
				String value = nodeToString( valueLit );
				Map<String,String> fieldMap ;
				if (codeMap.containsKey(field)) {
					fieldMap = codeMap.get(field);
				} else {
					fieldMap = new HashMap<String,String>();
				}
				String values = fieldMap.get(code);
				if( values == null ){					
					fieldMap.put(code, value);
				}else{
					fieldMap.put(code, values + ' ' + value);				
				}
				codeMap.put(field, fieldMap);
			}
		}
	};

	private final String queryKey = "query";
}
