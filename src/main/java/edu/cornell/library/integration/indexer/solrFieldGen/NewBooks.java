package edu.cornell.library.integration.indexer.solrFieldGen;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputField;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.indexer.JenaResultsToMarcRecord;
import edu.cornell.library.integration.indexer.utilities.SolrFields;
import edu.cornell.library.integration.indexer.utilities.SolrFields.BooleanSolrField;
import edu.cornell.library.integration.indexer.utilities.SolrFields.SolrField;
import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.Subfield;

/**
 * Look for various factors to identify books on new books shelves and acquisition
 * dates for recently acquired materials. Acquisition dates are less reliable the
 * more time has passed.
 * 
 */
public class NewBooks implements ResultSetToFields, SolrFieldGenerator {
	
	final static Boolean debug = false;

	@Override
	public Map<String, SolrInputField> toFields(
			Map<String, ResultSet> results, SolrBuildConfig config) throws Exception {

		MarcRecord bibRec = new MarcRecord(MarcRecord.RecordType.BIBLIOGRAPHIC);
		Map<String,MarcRecord> holdingRecs = new HashMap<>();

		for( String resultKey: results.keySet()){
			com.hp.hpl.jena.query.ResultSet rs = results.get(resultKey);
			while( rs.hasNext() ){
				QuerySolution sol = rs.nextSolution();


				if ( resultKey.equals("948")) {

					JenaResultsToMarcRecord.addDataFieldQuerySolution(bibRec,sol);

				} else if ( resultKey.equals("008") ) {

					JenaResultsToMarcRecord.addControlFieldQuerySolution(bibRec, sol);

				} else {
	 				String recordURI = ResultSetUtilities.nodeToString(sol.get("mfhd"));
					MarcRecord rec;
					if (holdingRecs.containsKey(recordURI)) {
						rec = holdingRecs.get(recordURI);
					} else {
						rec = new MarcRecord(MarcRecord.RecordType.HOLDINGS);
						holdingRecs.put(recordURI, rec);
					}
					if (resultKey.contains("Control")) {
						JenaResultsToMarcRecord.addControlFieldQuerySolution(rec,sol);
					} else {
						JenaResultsToMarcRecord.addDataFieldQuerySolution(rec,sol);
					}

				}
			}
		}
		bibRec.holdings.addAll(holdingRecs.values());
		SolrFields vals = generateSolrFields( bibRec, config );
		Map<String,SolrInputField> fields = new HashMap<>();
		for ( SolrField f : vals.fields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		for ( BooleanSolrField f : vals.boolFields )
			ResultSetUtilities.addField(fields, f.fieldName, f.fieldValue);		
		return fields;
	}

	@Override
	public String getVersion() { return "1.0"; }

	@Override
	public List<String> getHandledFields() { return Arrays.asList("007","948","holdings"); }

  	static Integer twoYearsAgo = Integer.valueOf(twoYearsAgo());
  	@Override
	public SolrFields generateSolrFields ( MarcRecord bib, SolrBuildConfig config ) {
	  	Collection<String> loccodes = new HashSet<>();
	  	Boolean newBooksZNote = false;

	  	SolrFields vals = new SolrFields();

	  	for (MarcRecord hold : bib.holdings)
	  		for (DataField f : hold.dataFields) if (f.tag.equals("852"))
	  			for (Subfield sf : f.subfields)
	  				if (sf.code.equals('k')) {
	  					if (sf.value.equalsIgnoreCase("new & noteworthy books"))
	  						vals.add(new SolrField("new_shelf","Olin Library New & Noteworthy Books"));
	  				} else if (sf.code.equals('b')) {
	  					loccodes.add(sf.value);
	  				} else if (sf.code.equals('z')) {
	  					String val = sf.value.toLowerCase();
	  					if (val.contains("new book") && val.contains("shelf"))
	  						newBooksZNote = true;
	  				}

	  	if (newBooksZNote)
	  		for (String loccode : loccodes)
	  			if (loccode.startsWith("afr"))
		  			vals.add(new SolrField("new_shelf","Africana Library New Books Shelf"));

	  	// Begin New Books Logic
	  	
	  	// Look for a holdings record that reflects a recent acquisition
	  	Boolean matchingMfhd = false;
	  	HOLD: for (MarcRecord hold : bib.holdings) {

	  		// Skip holdings that reflect items moved to the annex
	  		for (DataField f : hold.dataFields)     if (f.tag.equals("852"))
	  			for (Subfield sf : f.subfields)     if (sf.code.equals('x') && sf.value.contains("transfer"))
	  				for (String loccode : loccodes) if (loccode.endsWith(",anx"))
	  					continue HOLD;

	  		String date = null;
	  		for (ControlField f : hold.controlFields) if (f.tag.equals("005")) {
	  			if (f.value.length() < 6) continue HOLD;
	  			date = f.value.substring(0, 8);
	  			if (Integer.valueOf(date) < twoYearsAgo) continue HOLD;
	  		}
	  		matchingMfhd = true;
	  	}

	  	if ( ! matchingMfhd ) return vals;

	  	Collection<String> f948as = new HashSet<>();
	  	for (DataField f : bib.dataFields)   if (f.tag.equals("948") && f.ind1.equals('1'))
	  		for (Subfield sf : f.subfields)  if (sf.code.equals('a'))
	  			if (Integer.valueOf(sf.value) > twoYearsAgo)
	  				f948as.add(sf.value);

	  	if ( f948as.isEmpty() ) return vals;

	  	// Stop processing if record is microform
	  	for (ControlField f : bib.controlFields)
	  		if (f.tag.equals("007") && ! f.value.isEmpty() && f.value.substring(0,1).equals('h'))
	  			return vals;

	  	Integer acquiredDate = null;
	  	for (String f948a : f948as) {
	  		Integer date = Integer.valueOf(f948a.substring(0, 8));
	  		if (acquiredDate == null || acquiredDate < date)
	  			acquiredDate = date;
	  	}
	  	if (acquiredDate != null) {
		  	if (debug) System.out.println("Of "+f948as.size()+" recent 948a's, "+acquiredDate+" seems to be the most recent.");
		  	String date_s = acquiredDate.toString();
			vals.add(new SolrField("acquired",date_s));
			vals.add(new SolrField("acquired_month",date_s.substring(0,6)));
	  	}
		return vals;
	}
	
    public static String twoYearsAgo(  ) {
    	Calendar now = Calendar.getInstance();
    	String thisYear = new SimpleDateFormat("yyyy").format(now.getTime());
    	Integer twoYearsAgo = Integer.valueOf(thisYear) - 2;
    	String thisMonthDay = new SimpleDateFormat("MMdd").format(now.getTime());
    	return twoYearsAgo.toString() + thisMonthDay;
    }
}
