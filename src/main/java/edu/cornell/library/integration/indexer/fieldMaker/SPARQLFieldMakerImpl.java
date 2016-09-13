package edu.cornell.library.integration.indexer.fieldMaker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.resultSetToFields.ResultSetToFields;

/**
 * FieldMaker that runs a SPARQL query and uses the results
 * to make SolrInputFields.
 * 
 * @author bdc34
 *
 */
public class SPARQLFieldMakerImpl extends SPARQLFieldMakerBase{	
	
			
	/** Objects to process the results of the SPARQL queries into fields */
	List<ResultSetToFields> resultSetToFields;
	
	public SPARQLFieldMakerImpl() {
		super();
	}
	
	public SPARQLFieldMakerImpl setName(String name){
		super.name = name;
		return this;
	}
	
	public SPARQLFieldMakerImpl addLocalStoreQuery(String key, String query){
		if( this.localStoreQueries == null )			
			this.localStoreQueries = new HashMap<String,String>();
		
		this.localStoreQueries.put(key,query);
		return this;
	}

	public SPARQLFieldMakerImpl addMainStoreQuery(String key, String query){
		if( this.mainStoreQueries == null )
			this.mainStoreQueries = new HashMap<String, String>();
		this.mainStoreQueries.put(key, query);
		return this;		
	}

	public SPARQLFieldMakerImpl  addResultSetToFields(ResultSetToFields rs2f) {
		if( this.resultSetToFields == null )
			this.resultSetToFields = new ArrayList<ResultSetToFields>();		
		this.resultSetToFields.add( rs2f );
		return this;
	}

	
	/**
	 * Convert the result sets generated from running the SPARQL queries to
	 * SolrInputFields. 
	 */
	@Override
	protected Map<? extends String, ? extends SolrInputField> 
		resultSetsToSolrFields( Map<String, ResultSet> results, SolrBuildConfig config ) 
		throws Exception {
		
		Map<String, SolrInputField> fields = new HashMap<String,SolrInputField>();
		
		if( resultSetToFields != null){
			for( ResultSetToFields r2f : resultSetToFields ){
				if( r2f != null ){					
					Map<String,SolrInputField> newFields =r2f.toFields( results, config ) ;
					if (config.isDebugClass(r2f.getClass()) ) {
						System.out.println(r2f.getClass().getName()+" fields derived:");
						dumpFieldsToStdout(newFields);
					}
					if( newFields != null)
						fields.putAll( newFields);
				}
			}
		}
		return fields;
		
	}		

	/* debug utility */
	private static void dumpFieldsToStdout( Map<String,SolrInputField> newFields ) {
		for (Entry<String,SolrInputField> entry : newFields.entrySet()) {
			System.out.println(entry.getKey());
			for (Object value : entry.getValue().getValues())
				System.out.println("\t"+value.toString());
		}
		
	}

	static final Log log = LogFactory.getLog( SPARQLFieldMakerImpl.class);
}
