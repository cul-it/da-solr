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
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * Get values for subfields List in order and put into
 * a SolrInputField  
 */
public class SubfieldCodeMaker implements FieldMaker {
	
	String subfieldCodes = "";
	String marcFieldNumber = null;
	String solrFieldName = null;
	
	public String getSubfieldCodes() {
		return subfieldCodes;
	}
	public SubfieldCodeMaker setSubfieldCodes(String subfieldCodes) {
		this.subfieldCodes = subfieldCodes;
		return this;
	}

	public SubfieldCodeMaker setMarcFieldNumber(String marcFieldNumber) {
		this.marcFieldNumber = marcFieldNumber;
		return this;
	}

	public SubfieldCodeMaker setSolrFieldName(String solrFieldName) {
		this.solrFieldName = solrFieldName;
		return this;
	}
 		

	private String getName() {
		return SubfieldCodeMaker.class.getSimpleName() +
				" for MARC field " + marcFieldNumber + 
				" and codes " + subfieldCodes;
	}
	
	@Override
	public Map<? extends String, ? extends SolrInputField> buildFields(
			String recordURI, RDFService mainStore,
			RDFService localStore) throws Exception {
		//need to setup query once the recordURI is known
		//subfield values filtered to only the ones requested
		String query = 
				"SELECT ?code ?value WHERE { \n"+
				"<"+recordURI+"> <http://marcrdf.library.cornell.edu/canonical/0.1/hasField> ?field . \n"+
			    "?field <http://marcrdf.library.cornell.edu/canonical/0.1/tag> \"" + marcFieldNumber + "\" . \n"+
				"?field <http://marcrdf.library.cornell.edu/canonical/0.1/hasSubfield> ?sfield .\n"+
				"?sfield <http://marcrdf.library.cornell.edu/canonical/0.1/value> ?value .\n"+
				"?sfield <http://marcrdf.library.cornell.edu/canonical/0.1/code> ?code\n"+
				"FILTER( CONTAINS( \"" + getSubfieldCodes() + "\" , ?code) )\n"+
				"} ORDER BY ?sfield";
										
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
				
			Map<String,String> codeMap = new HashMap<String,String>();
			ResultSet rs = results.get(queryKey);
			while( rs.hasNext() ){
				addSolToMap( codeMap, rs.nextSolution() );				
			}
			
			String sortedVals = "";			
			for( char code : getSubfieldCodes().toCharArray()){
				String values = codeMap.get(Character.toString( code ));
				if( values != null ) {
					if (sortedVals.equals("")) {
						sortedVals = values;
					} else {
						sortedVals = sortedVals + " " + values;
					}
				}
			}
			SolrInputField solrField = new SolrInputField(sortedVals);			
			
			return Collections.singletonMap(solrFieldName, solrField);
		}		

		private void addSolToMap(Map<String,String> codeMap, QuerySolution sol){
			Literal codeLit = sol.getLiteral("code");
			Literal valueLit = sol.getLiteral("value");
			if( codeLit != null || valueLit != null ){
				String code = codeLit.getLexicalForm();
				String value = valueLit.getLexicalForm();
				String values = codeMap.get(code);
				if( values == null ){					
					codeMap.put(code, value);
				}else{
					codeMap.put(code, values + ' ' + value);				
				}	
			}
		}
	};

	private final String queryKey = "query";
}
