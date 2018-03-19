package edu.cornell.library.integration.indexer.solrFieldGen;

import static edu.cornell.library.integration.indexer.solrFieldGen.ResultSetUtilities.nodeToString;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.Config;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Currently, the only record types are "Catalog" and "Shadow", where shadow records are 
 * detected through a 948‡h note. Blacklight searches will be filtered to type:Catalog,
 * so only records that should NOT be returned in Blacklight work-level searches should
 * vary from this.
 */
public class RecordType implements ResultSetToFields, SolrFieldGenerator {

	protected boolean debug = false;
	
	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, Config config) throws Exception {

		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		Map<String,MarcRecord> holdingRecs = new HashMap<>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ) {
				QuerySolution sol = rs.nextSolution();

				if ( resultKey.equals("948") ) {
					JenaResultsToMarcRecord.addDataFieldQuerySolution(bibRec,sol);
				} else {
					String recordURI = nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (holdingRecs.containsKey(recordURI)) {
						rec = holdingRecs.get(recordURI);
					} else {
						rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
						rec.id = recordURI.substring(recordURI.lastIndexOf('/')+1);
						holdingRecs.put(recordURI, rec);
					}
					JenaResultsToMarcRecord.addDataFieldQuerySolution(rec,sol);
				}
			}
		}
		bibRec.holdings.addAll(holdingRecs.values());
		SolrFields vals = generateSolrFields( bibRec, config );
		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("948","holdings"); }

	@Override
	public SolrFields generateSolrFields( MarcRecord rec, Config unused ) {

		Boolean isShadow = false;

		for ( DataField f : rec.dataFields )
			if ( f.tag.equals("948") )
				for ( Subfield sf : f.subfields )
					if ( sf.code.equals('h') && sf.value.equalsIgnoreCase("public services shadow record") )
						isShadow = true;

		for ( MarcRecord hRec : rec.holdings ) 
			for ( DataField f : hRec.dataFields )
				if ( f.tag.equals("852") )
					for ( Subfield sf : f.subfields )
						if ( sf.code.equals('x') && sf.value.equalsIgnoreCase("public services shadow record") )
							isShadow = true;

		SolrFields sfs = new SolrFields();
		sfs.add(new SolrField("type",isShadow?"Shadow":"Catalog"));
		sfs.add(new SolrField("source","Voyager"));
		return sfs;
	}

}
