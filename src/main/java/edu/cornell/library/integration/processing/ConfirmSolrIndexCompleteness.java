package edu.cornell.library.integration.processing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.IndexRecordListComparison;


/**
 * Generate reports on:
 *  what Bib IDs are in Voyager but not in the Solr index
 *  what Bib IDs are in the Solr index but not in Voyager
 *  what MFHD IDs are in Voyager but not in the Solr index
 *  what MFHD IDs are in the Solr index but not in Voyager  
 *
 */
public class ConfirmSolrIndexCompleteness  {


	
	private Config config;
	
	public ConfirmSolrIndexCompleteness(Config config) {
        this.config = config;
    }
	
    /**
	 * This main method takes the standard command line parameters
	 * for VoyagerToSolrConfiguration. 
	 */
	public static void main(String[] args) throws Exception  {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.add("solrUrl");
        Config config = Config.loadConfig(requiredArgs);
        ConfirmSolrIndexCompleteness csic = new ConfirmSolrIndexCompleteness( config );
        int numberOfMissingBibs = csic.doCompletnessCheck( config.getSolrUrl() );
        System.exit(numberOfMissingBibs);  //any bibs missing from index should cause failure status
	}
	
	public int doCompletnessCheck(String coreUrl) throws Exception {
		System.out.println("Comparing current Voyager record lists \n"
		        + "with contents of index at: " + coreUrl+"\n");

		IndexRecordListComparison c = new IndexRecordListComparison(this.config);

		reportList( c.bibsInIndexNotVoyager(),
				"Bib ids in the index but no longer unsuppressed in Voyager.");
		reportList( c.bibsInVoyagerNotIndex(),
				"Bib ids unsuppressed in Voyager but not in the index.");
		reportList( c.mfhdsInIndexNotVoyager(),
				"Mfhd (holdings) ids in the index but no longer unsuppressed in Voyager - bib ids in parens.");
		reportList( c.mfhdsInVoyagerNotIndex(),
				"Mfhd (holdings) ids unsuppressed in Voyager but not in the index.");

		return c.bibsInVoyagerNotIndex().size();
	}

	private static void reportList(Map<Integer,Integer> idMap, String reportDesc) {

		List<String> ids = idMap.keySet().stream().map(id->id+" ("+idMap.get(id)+")")
				.collect(Collectors.toList());

		// Print summary to stdout
		if (ids.size() > 0){
			System.out.println(reportDesc);
			System.out.println( String.join(", ",ids)+"\n");
		}
		
		idMap.clear();

	}

	private static void reportList( Set<Integer> idList, String reportDesc) {
	
		List<String> ids = idList.stream().map(Object::toString).collect(Collectors.toList());

		// Print summary to Stdout
		if (ids.size() > 0){
		    System.out.println(reportDesc);
		    System.out.println( String.join(", ",ids)+"\n" );
		}

	}

}
