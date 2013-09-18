package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilies.IndexRecordListComparison;
import edu.cornell.library.integration.indexer.utilies.IndexingUtilities;

public class IdentifyDeletedRecords {
	
	private final String davUrl = "http://culdata.library.cornell.edu/data";

	DavService davService;
			
	public static void main(String[] args)  {
		String coreUrl = "http://fbw4-dev.library.cornell.edu:8080/solr/test";
		if (args.length >= 1)
			coreUrl = args[0];
		if (args.length >= 3) {
			DavService davService = DavServiceFactory.getDavService();
			try {
				Path currentVoyagerBibList = davService.getNioPath(args[1]);
				Path currentVoyagerMfhdList = davService.getNioPath(args[2]);
				new IdentifyDeletedRecords(coreUrl,currentVoyagerBibList,currentVoyagerMfhdList);
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
		    try{		
		        new IdentifyDeletedRecords(coreUrl);
		    }catch( Exception e){
		        e.printStackTrace();
		        System.exit(1);
		    }
		}
	}
	
	public IdentifyDeletedRecords(String coreUrl, Path currentVoyagerBibList, Path currentVoyagerMfhdList) {

		davService = DavServiceFactory.getDavService();
		System.out.println("Comparing to contents of index at: " + coreUrl);

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(coreUrl, currentVoyagerBibList, currentVoyagerMfhdList);
		
		produceReport(c);

	}
	
    
	public IdentifyDeletedRecords(String coreUrl) throws Exception {
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
			
			produceReport(c);
		}
 	}

	// Based on the IndexRecordListComparison, bib records to be updated and deleted are printed to STDOUT
	// and also written to report files on the webdav server in the /updates/bib.deletes and /updates/bibupdates
	// folders. The report files have post-pended dates in their file names.
	private void produceReport( IndexRecordListComparison c ) {

		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

		// bibs in index not voyager
		if (c.bibsInIndexNotVoyager.size() > 0) {
//			System.out.println("bibids to be deleted from Solr");
			Integer[] ids = c.bibsInIndexNotVoyager.toArray(new Integer[ c.bibsInIndexNotVoyager.size() ]);
			Arrays.sort( ids );

			StringBuilder sb = new StringBuilder();
			for( Integer id: ids ) {
//				System.out.println(id);
				sb.append(id);
				sb.append("\n");
			}

			String deleteReport = sb.toString();
			try {
				davService.saveFile(davUrl+"/updates/bib.deletes/bibListForDelete-"+ currentDate + ".txt",
						new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
		}

		
		// CurrentIndexMfhdList should now only contain mfhds to be deleted.
		if (c.mfhdsInIndexNotVoyager.size() > 0) {
			Iterator<Integer> bibids = c.mfhdsInIndexNotVoyager.values().iterator();
			Set<Integer> update_bibids = new TreeSet<Integer>();
//			System.out.println("bibids to be updated in solr");
			while (bibids.hasNext())
				update_bibids.add(bibids.next());
			bibids = update_bibids.iterator();
			StringBuilder sb = new StringBuilder();
			while (bibids.hasNext()) {
				Integer bibid = bibids.next();
//				System.out.println(bibid);
				sb.append(bibid);
				sb.append("\n");
			}

			String updateReport = sb.toString();
			try {
				davService.saveFile(davUrl+"/updates/bib.updates/bibListForUpdate-"+ currentDate + ".txt",
						new ByteArrayInputStream(updateReport.getBytes("UTF-8")));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
	}
	
	public static String findMostRecentBibFile(DavService davService , String davBaseUrl ) throws Exception{        
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + "/voyager/bib/unsuppressed" , "unsuppressedBibId");                    
        } catch (Exception cause) {
            throw new Exception("Could not find most recent bib file", cause);
        }        
    }

    public static String findMostRecentMfhdFile(DavService davService, String davBaseUrl) throws Exception{                
        try {
            return IndexingUtilities.findMostRecentFile( davService, davBaseUrl + "/voyager/mfhd/unsuppressed", "unsuppressedMfhdId");            
        } catch (Exception e) {
            throw new Exception( "Could not get most recent Mfhd holding file.", e);
        }    
    }
}
