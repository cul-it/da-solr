package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison;
import static edu.cornell.library.integration.indexer.utilities.IndexingUtilities.optimizeIndex;


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
	private VoyagerToSolrConfiguration config;
	private DavService davService;
    private String reportsUrl;
	
	public ConfirmSolrIndexCompleteness(VoyagerToSolrConfiguration config) throws IOException {
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
        VoyagerToSolrConfiguration config = VoyagerToSolrConfiguration.loadConfig(args);
        ConfirmSolrIndexCompleteness csic = new ConfirmSolrIndexCompleteness( config );
        int numberOfMissingBibs = csic.doCompletnessCheck( config.getSolrUrl() );
        if (numberOfMissingBibs == 0) 
        	optimizeIndex( config.getSolrUrl() );
        System.exit(numberOfMissingBibs);  //any bibs missing from index should cause failure status
	}
	
	public int doCompletnessCheck(String coreUrl, Path currentVoyagerBibList, Path currentVoyagerMfhdList) throws Exception {
		System.out.println("Comparing \n" 
		        + currentVoyagerBibList.toUri()  + " and \n" 
		        + currentVoyagerMfhdList.toUri() + "\n"
		        + "with contents of index at: " + coreUrl);

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
		produceReport(davUrl,c);
		
		return c.bibsInVoyagerNotIndex.size();
	}	
    
	public int  doCompletnessCheck(String coreUrl) throws Exception {
		
		Path currentVoyagerBibList = null;
		Path currentVoyagerMfhdList = null;
		
		String mostRecentBibFile =  
		        FileNameUtils.findMostRecentUnsuppressedBibIdFile(config, getDavService());
		
		try{
			if (mostRecentBibFile != null) {
				System.out.println("Most recent bib file identified as: "+ mostRecentBibFile);
				currentVoyagerBibList = getDavService().getNioPath( mostRecentBibFile );
			}else{
			    System.out.println("No recent bib file found.");
			    System.exit(1);
			}
		} catch (Exception e) {
		    throw new Exception( "Could not get most recent bib file from '" + mostRecentBibFile +"'", e);
		}
		
		String mostRecentMfhdFile = 
		        FileNameUtils.findMostRecentUnsuppressedMfhdIdFile(config, getDavService()); 
		try {		
			if (mostRecentMfhdFile != null) {
				System.out.println("Most recent mfhd file identified as: " + mostRecentMfhdFile);
				currentVoyagerMfhdList = getDavService().getNioPath( mostRecentMfhdFile);
			}else{
			    System.out.println("No recent Mfhd holdings file found.");
                System.exit(1);
			}
		} catch (Exception e) {
			throw new Exception( "Could not get most recent Mfhd holding file '" + mostRecentMfhdFile + "'" , e);
		}		

		int numberOfMissingBibs = doCompletnessCheck( coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
        return numberOfMissingBibs;
 	}

	// Based on the IndexRecordListComparison, bib records to be updated and deleted are printed to STDOUT
	// and also written to report files on the webdav server in the /updates/bib.deletes and /updates/bibupdates
	// folders. The report files have post-pended dates in their file names.
	private void produceReport(  String davUrl, IndexRecordListComparison c ) throws Exception {

		System.out.println();
		
		reportList( c.bibsInIndexNotVoyager,"bibsInIndexNotVoyager.txt",
				"Bib ids in the index but no longer unsuppressed in Voyager.");

		reportList( c.bibsInVoyagerNotIndex,"bibsInVoyagerNotIndex.txt",
				"Bib ids unsuppressed in Voyager but not in the index.");

		reportList( c.mfhdsInIndexNotVoyager,"mfhdsInIndexNotVoyager.txt",
				"Mfhd (holdings) ids in the index but no longer unsuppressed in Voyager - bib ids in parens.");

		reportList( c.mfhdsInVoyagerNotIndex,"mfhdsInVoyagerNotIndex.txt",
				"Mfhd (holdings) ids unsuppressed in Voyager but not in the index.");
		
	}

	private void reportList(Map<Integer,Integer> idMap, String reportFilename, String reportDesc) throws Exception {
	    
		Set<Integer> idList = idMap.keySet();
		Integer[] ids = idList.toArray(new Integer[ idList.size() ]);
		Arrays.sort( ids );
				
		StringBuilder sb = new StringBuilder();
		List<String> display_examples = new ArrayList<String>();
		for( int i = 0; i < ids.length; i++ ) {
			Integer id = ids[i];
			String pair = id +" ("+idMap.get(id)+")";
			if (i < 10) {
				display_examples.add( pair );
			}
			sb.append(pair).append('\n');
		}
		
		String url = reportsUrl + reportFilename;
		
		// Print summary to stdout
		if (idList.size() > 0){
            System.out.println(reportDesc);
            System.out.println( StringUtils.join(display_examples, ", ") );
            if (idList.size() > 10)
                System.out.println("(for the full list, see "+ url + ")");
            System.out.println("");
		}		           
		
		// Save file on WEBDAV		 
		try {
			getDavService().saveFile( url,
					new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));			
		} catch (Exception e) {
		    throw new Exception("Problem saving report " + url ,e);
		}
		idMap.clear();
	}

	
	private void reportList( Set<Integer> idList, String reportFilename, String reportDesc) throws Exception {
		Integer[] ids = idList.toArray(new Integer[ idList.size() ]);
		Arrays.sort( ids );
	
		StringBuilder sb = new StringBuilder();
		List<Integer> display_examples = new ArrayList<Integer>();
		for( int i = 0; i < ids.length; i++ ) {
			Integer id = ids[i];
			if (i < 10) {
				display_examples.add(id);				
			}
			sb.append(id).append("\n");
		}
				
		String url = reportsUrl + reportFilename;
		// Print summary to Stdout
		if (idList.size() > 0){
		    System.out.println(reportDesc);
		    System.out.println( StringUtils.join( display_examples, ", ") );
		    if (idList.size() > 10)
	            System.out.println("(for the full list, see "+ url +")");
		    System.out.println("");
		}				        
		
		// Save file on WEBDAV
		try {
			getDavService().saveFile( url , 
					new ByteArrayInputStream(sb.toString().getBytes("UTF-8")));
		} catch (Exception e) {
		    throw new Exception("Problem saving report " + url, e);			
		}
		
	}
	
	private DavService getDavService(){
	    return davService;	
	}
	
}
