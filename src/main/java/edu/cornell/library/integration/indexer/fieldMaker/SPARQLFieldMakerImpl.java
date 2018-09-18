package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.solrFieldGen.ResultSetToFields;
import edu.cornell.library.integration.indexer.utilities.Config;

/**
 * FieldMaker that runs a SPARQL query and uses the results
 * to make SolrInputFields.
 * 
 * @author bdc34
 *
 */
public class SPARQLFieldMakerImpl extends SPARQLFieldMakerBase{	
	
			
	/** Objects to process the results of the SPARQL queries into fields */
	private List<ResultSetToFields> resultSetToFields;
	
	public SPARQLFieldMakerImpl() {
		super();
	}

	public SPARQLFieldMakerImpl addQuery(String key, String query){
		if( this.queries == null )
			this.queries = new HashMap<>();
		this.queries.put(key, query);
		return this;		
	}

	public SPARQLFieldMakerImpl  addResultSetToFields(ResultSetToFields rs2f) {
		if( this.resultSetToFields == null )
			this.resultSetToFields = new ArrayList<>();		
		this.resultSetToFields.add( rs2f );
		return this;
	}

	/**
	 * Convert the result sets generated from running the SPARQL queries to
	 * SolrInputFields. 
	 */
	@Override
	protected Map<? extends String, ? extends SolrInputField> 
		resultSetsToSolrFields( Map<String, ResultSet> results, Config config ) 
		throws Exception {
		
		Map<String, SolrInputField> fields = new HashMap<>();
		
		if( resultSetToFields != null){
			for( ResultSetToFields r2f : resultSetToFields ){
				if( r2f != null ){					
					Map<String,SolrInputField> newFields =r2f.toFields( results, config ) ;
					if( newFields != null)
						fields.putAll( newFields);
				}
			}
		}
		return fields;
		
	}		
}
