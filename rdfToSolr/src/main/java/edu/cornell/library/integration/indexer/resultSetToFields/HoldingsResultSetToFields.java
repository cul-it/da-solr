package edu.cornell.library.integration.indexer.resultSetToFields;

import static edu.cornell.library.integration.indexer.resultSetToFields.ResultSetUtilities.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.MarcRecord;
import edu.cornell.library.integration.indexer.MarcRecord.FieldSet;

/**
 * processing title result sets into fields title_t, title_vern_display, subtitle_t, 
 * subtitle_vern_display, and title_sort. The rest of the title fields don't require 
 * specialized handling. 
 */
public class HoldingsResultSetToFields implements ResultSetToFields {

	@Override
	public Map<? extends String, ? extends SolrInputField> toFields(
			Map<String, ResultSet> results) throws Exception {
		
		//The results object is a Map of query names to ResultSets that
		//were created by the fieldMaker objects.

		Map<String,MarcRecord> recs = new HashMap<String,MarcRecord>();
		

		for( String resultKey: results.keySet()){
			ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();
				String recordURI = nodeToString(sol.get("mfhd"));
				MarcRecord rec;
				if (recs.containsKey(recordURI)) {
					rec = recs.get(recordURI);
				} else {
					rec = new MarcRecord();
				}
				rec.addDataFieldQuerySolution(sol);
				recs.put(recordURI, rec);
			}
//			rec.addDataFieldResultSet(rs);
		}
		
		for( String holdingURI: recs.keySet() ) {
			MarcRecord rec = recs.get(holdingURI);
			System.out.println(rec.toString());
			
			Map<Integer,FieldSet> sortedFields = rec.matchAndSortDataFields();
			
			
		}
		
		
		Map<String,SolrInputField> solrFields = new HashMap<String,SolrInputField>();

		
		return solrFields;
	}


}
