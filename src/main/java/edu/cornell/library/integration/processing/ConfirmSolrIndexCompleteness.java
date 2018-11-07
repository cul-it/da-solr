package edu.cornell.library.integration.processing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.cornell.library.integration.utilities.Config;
import edu.cornell.library.integration.utilities.IndexRecordListComparison;
import edu.cornell.library.integration.webdav.DavService;
import edu.cornell.library.integration.webdav.DavServiceFactory;


/**
 * Generate reports on:
 *  what Bib IDs are in Voyager but not in the Solr index
 *  what Bib IDs are in the Solr index but not in Voyager
 *  what MFHD IDs are in Voyager but not in the Solr index
 *  what MFHD IDs are in the Solr index but not in Voyager  
 *
 */
public class ConfirmSolrIndexCompleteness  {


	
	private String davUrl;	
	private Config config;
	private DavService davService;
    private String reportsUrl;
	
	public ConfirmSolrIndexCompleteness(Config config) throws IOException {
        this.config = config;
        this.davUrl = config.getWebdavBaseUrl();
        this.reportsUrl = davUrl + "/" + config.getDailyReports() + "/";
        this.davService = DavServiceFactory.getDavService(config);
    }
	
    /**
	 * This main method takes the standard command line parameters
	 * for VoyagerToSolrConfiguration. 
	 */
	public static void main(String[] args) throws Exception  {
		List<String> requiredArgs = Config.getRequiredArgsForDB("Current");
		requiredArgs.addAll(Config.getRequiredArgsForWebdav());
		requiredArgs.add("solrUrl");
        Config config = Config.loadConfig(args, requiredArgs);
        ConfirmSolrIndexCompleteness csic = new ConfirmSolrIndexCompleteness( config );
        int numberOfMissingBibs = csic.doCompletnessCheck( config.getSolrUrl() );
        System.exit(numberOfMissingBibs);  //any bibs missing from index should cause failure status
	}
	
	public int doCompletnessCheck(String coreUrl) throws Exception {
		System.out.println("Comparing current Voyager record lists \n"
		        + "with contents of index at: " + coreUrl);

		IndexRecordListComparison c = new IndexRecordListComparison(config);
		produceReport(c);
		
		return c.bibsInVoyagerNotIndex().size();
	}

	// Based on the IndexRecordListComparison, bib records to be updated and deleted are printed to STDOUT
	// and also written to report files on the webdav server in the /updates/bib.deletes and /updates/bibupdates
	// folders. The report files have post-pended dates in their file names.
	private void produceReport(  IndexRecordListComparison c ) throws Exception {

		System.out.println();
		
		reportList( c.bibsInIndexNotVoyager(),"bibsInIndexNotVoyager.txt",
				"Bib ids in the index but no longer unsuppressed in Voyager.");

		reportList( c.bibsInVoyagerNotIndex(),"bibsInVoyagerNotIndex.txt",
				"Bib ids unsuppressed in Voyager but not in the index.");

		reportList( c.mfhdsInIndexNotVoyager(),"mfhdsInIndexNotVoyager.txt",
				"Mfhd (holdings) ids in the index but no longer unsuppressed in Voyager - bib ids in parens.");

		reportList( c.mfhdsInVoyagerNotIndex(),"mfhdsInVoyagerNotIndex.txt",
				"Mfhd (holdings) ids unsuppressed in Voyager but not in the index.");
		
	}

	private void reportList(Map<Integer,Integer> idMap, String reportFilename, String reportDesc) throws Exception {

		List<String> ids = idMap.keySet().stream().map(id->id+" ("+idMap.get(id)+")")
				.collect(Collectors.toList());

		String url = reportsUrl + reportFilename;

		// Print summary to stdout
		if (ids.size() > 0){
			List<String> display_examples = ids.stream().limit(10).collect(Collectors.toList());
            System.out.println(reportDesc);
            System.out.println( String.join(", ",display_examples));
            if (ids.size() > 10)
                System.out.println("(for the full list, see "+ url + ")");
            System.out.println("");
		}
		
		idMap.clear();

		// Save file on WEBDAV
		try ( ByteArrayInputStream is = new ByteArrayInputStream(
				String.join("\n",ids).getBytes(StandardCharsets.UTF_8)) ) {
			davService.saveFile( url,is);
		}

	}

	private void reportList( Set<Integer> idList, String reportFilename, String reportDesc) throws Exception {
	
		List<String> ids = idList.stream().map(Object::toString).collect(Collectors.toList());
		String url = reportsUrl + reportFilename;

		// Print summary to Stdout
		if (ids.size() > 0){
			List<String> display_examples = ids.stream().limit(10).collect(Collectors.toList());
		    System.out.println(reportDesc);
		    System.out.println( String.join(", ",display_examples) );
		    if (ids.size() > 10)
	            System.out.println("(for the full list, see "+ url +")");
		    System.out.println("");
		}				        

		try ( ByteArrayInputStream is = new ByteArrayInputStream(
				String.join("\n",ids).getBytes(StandardCharsets.UTF_8)) ) {
			davService.saveFile( url,is);
		}

	}

}
