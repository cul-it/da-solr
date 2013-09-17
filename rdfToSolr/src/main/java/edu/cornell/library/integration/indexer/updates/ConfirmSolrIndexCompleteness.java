package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilies.IndexRecordListComparison;
import edu.cornell.library.integration.indexer.utilies.IndexingUtilities;

public class ConfirmSolrIndexCompleteness {
	
	private final String davUrl = "http://culdata.library.cornell.edu/data";

	DavService davService;
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args)  {
		String coreUrl = "http://fbw4-dev.library.cornell.edu:8080/solr/test";
		if (args.length >= 1)
			coreUrl = args[0];
		if (args.length >= 3) {
			DavService davService = DavServiceFactory.getDavService();
			try {
				Path currentVoyagerBibList = davService.getNioPath(args[1]);
				Path currentVoyagerMfhdList = davService.getNioPath(args[2]);
				new ConfirmSolrIndexCompleteness(coreUrl,currentVoyagerBibList,currentVoyagerMfhdList);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
		    try{		
		        new ConfirmSolrIndexCompleteness(coreUrl);
		    }catch( Exception e){
		        e.printStackTrace();
		        System.exit(1);
		    }
		}
	}
	
	public ConfirmSolrIndexCompleteness(String coreUrl, Path currentVoyagerBibList, Path currentVoyagerMfhdList) {

		davService = DavServiceFactory.getDavService();
		System.out.println("Comparing to contents of index at: " + coreUrl);

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
		produceReport(davService,davUrl,c);

	}
	
    
	public ConfirmSolrIndexCompleteness(String coreUrl) throws Exception {
		davService = DavServiceFactory.getDavService();
		Path currentVoyagerBibList = null;
		Path currentVoyagerMfhdList = null;
		
		String mostRecentBibFile = findMostRecentBibFile(davService, davUrl);		
		try{
			if (mostRecentBibFile != null) {
				System.out.println("Most recent bib file identified as: "+ mostRecentBibFile);
				currentVoyagerBibList = davService.getNioPath( mostRecentBibFile );
			}else{
			    System.out.println("No recent bib file found.");
			    System.exit(1);
			}
		} catch (Exception e) {
		    throw new Exception( "Could not get most recent bib file from '" + mostRecentBibFile +"'", e);
		}
		
		String mostRecentMfhdFile = findMostRecentMfhdFile(davService, davUrl);
		try {		
			if (mostRecentMfhdFile != null) {
				System.out.println("Most recent mfhd file identified as: " + mostRecentMfhdFile);
				currentVoyagerMfhdList = davService.getNioPath( mostRecentMfhdFile);
			}else{
			    System.out.println("No recent Mfhd holdings file found.");
                System.exit(1);
			}
		} catch (Exception e) {
			throw new Exception( "Could not get most recent Mfhd holding file '" + mostRecentMfhdFile + "'" , e);
		}
		
		if ((currentVoyagerBibList != null) && (currentVoyagerMfhdList != null)) {
			System.out.println("Comparing to contents of index at: " + coreUrl);
			
			IndexRecordListComparison c = new IndexRecordListComparison();
			c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
			produceReport(davService,davUrl,c);
		}
 	}

	// Based on the IndexRecordListComparison, bib records to be updated and deleted are printed to STDOUT
	// and also written to report files on the webdav server in the /updates/bib.deletes and /updates/bibupdates
	// folders. The report files have post-pended dates in their file names.
	private void produceReport( DavService davService, String davUrl, IndexRecordListComparison c ) {

		System.out.println();
		
		reportList(davService,davUrl,
				c.bibsInIndexNotVoyager,"bibsInIndexNotVoyager.txt",
				"Bib ids in the index but no longer unsuppressed in Voyager.");

		reportList(davService,davUrl,
				c.bibsInVoyagerNotIndex,"bibsInVoyagerNotIndex.txt",
				"Bib ids unsuppressed in Voyager but not in the index.");

		reportList(davService,davUrl,
				c.mfhdsInIndexNotVoyager,"mfhdsInIndexNotVoyager.txt",
				"Mfhd (holdings) ids in the index but no longer unsuppressed in Voyager - bib ids in parens.");

		reportList(davService,davUrl,
				c.mfhdsInVoyagerNotIndex,"mfhdsInVoyagerNotIndex.txt",
				"Mfhd (holdings) ids unsuppressed in Voyager but not in the index.");
		
	}

	private static void reportList(DavService davService,String davUrl,Map<Integer,Integer> idMap,String reportFilename, String reportDesc) {
		Set<Integer> idList = idMap.keySet();
		Integer[] ids = idList.toArray(new Integer[ idList.size() ]);
		Arrays.sort( ids );

		if (idList.size() > 0)
			System.out.println(reportDesc);
		StringBuilder sb = new StringBuilder();
		List<String> display_examples = new ArrayList<String>();
		for( int i = 0; i < ids.length; i++ ) {
			Integer id = ids[i];
			if (i < 10) {
				display_examples.add(id +" ("+idMap.get(id)+")");
			}
			sb.append(id + " ("+idMap.get(id)+")\n");
		}
		StringBuilder sb1 = new StringBuilder();
		Iterator<String> i = display_examples.iterator();
		while (i.hasNext()) {
			sb1.append(i.next());
			if (i.hasNext())
				sb1.append(", ");
		}
		System.out.println(sb1.toString());
		if (idList.size() > 10)
			System.out.println("(for the full list, see "+davUrl+"/reports/voyager/"+reportFilename+")");

		String deleteReport = sb.toString();
		try {
			davService.saveFile(davUrl+"/reports/voyager/"+reportFilename,
					new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (idList.size() > 0)
			System.out.println("");
	}

	private static void reportList(DavService davService,String davUrl,Set<Integer> idList,String reportFilename, String reportDesc) {

		Integer[] ids = idList.toArray(new Integer[ idList.size() ]);
		Arrays.sort( ids );

		if (idList.size() > 0)
			System.out.println(reportDesc);
		StringBuilder sb = new StringBuilder();
		List<Integer> display_examples = new ArrayList<Integer>();
		for( int i = 0; i < ids.length; i++ ) {
			Integer id = ids[i];
			if (i < 10) {
				display_examples.add(id);				
			}
			sb.append(id);
			sb.append("\n");
		}
		StringBuilder sb1 = new StringBuilder();
		Iterator<Integer> i = display_examples.iterator();
		while (i.hasNext()) {
			sb1.append(i.next());
			if (i.hasNext())
				sb1.append(", ");
		}
		System.out.println(sb1.toString());

		if (idList.size() > 10)
			System.out.println("(for the full list, see "+davUrl+"/reports/voyager/"+reportFilename+")");

		String deleteReport = sb.toString();
		try {
			davService.saveFile(davUrl+"/reports/voyager/"+reportFilename,
					new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (idList.size() > 0)
			System.out.println("");
	}
	
	private static String findMostRecentBibFile(DavService davService , String davBaseUrl ) throws Exception{        
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + "/voyager/bib/unsuppressed" , "unsuppressedBibId");                    
        } catch (Exception cause) {
            throw new Exception("Could not find most recent bib file", cause);
        }        
    }

    private static String findMostRecentMfhdFile(DavService davService, String davBaseUrl) throws Exception{                
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + "/voyager/mfhd/unsuppressed", "unsuppressedMfhdId");            
        } catch (Exception e) {
            throw new Exception( "Could not get most recent Mfhd holding file.", e);
        }    
    }
}
