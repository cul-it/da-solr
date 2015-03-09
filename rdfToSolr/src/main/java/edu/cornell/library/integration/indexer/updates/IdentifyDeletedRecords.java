package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.cornell.library.integration.ilcommons.configuration.VoyagerToSolrConfiguration;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.ilcommons.util.FileNameUtils;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison;

/**
 * Identify records that have been deleted from Voyager. This is done
 * by comparing the list of active BIB and MFHD records in Voyager
 * with the list of BIB IDs and MFHD IDs in the Solr index.
 *   
 * BIB ID that need to be deleted = 
 *   BIBIDs in Voyager SET_COMPLEMENT BIBIDs in Solr
 * 
 * BIB IDs that need to be updated = 
 *   BIBIDs in Solr SET_COMPLEMENT BIBIDs in Voyager
 *   +
 *   
 *   
 *   
 * "Deleted" records for this purpose refers to both suppressed and 
 * deleted bib and holdings record IDs.
 * 
 * The lists are generated by comparing lists from step 1 
 * (which is edu.cornell.library.integration.GetAllSuppressionsFromCatalog )
 * with the contents of the Solr index. This step doesn't need to run 
 * immediately after step 1.
 */
public class IdentifyDeletedRecords {
	
	DavService davService;
	VoyagerToSolrConfiguration config;

	public static void main(String[] args)  {

         try{        
             new IdentifyDeletedRecords( VoyagerToSolrConfiguration.loadConfig( args ));
         }catch( Exception e){
             e.printStackTrace();
             System.exit(1);
         }
	}	
	
    /**
     * Identify the most recent BIB and MFHD files on the WEBDAV,
     * and the compare their contents with the records in the Solr
     * index. The write to WEBDAV a file of BIB IDs that need 
     * to be deleted and a file of BIB IDs that need to be updated.
     * 
     * The BIB IDs that need to be deleted are due to the fact that
     * they exist in Solr but do not exist in Voyager.
     * 
     * The BIB IDs that need to be updated are due to the fact that 
     * their holdings records exist in Solr but do not exist in
     * Voyager.
     */
	public IdentifyDeletedRecords(VoyagerToSolrConfiguration config) throws Exception {
	    this.config = config;
		davService = DavServiceFactory.getDavService(config);
		
		Path currentVoyagerBibList = null;
		Path currentVoyagerMfhdList = null;
		
		String mostRecentBibFile = FileNameUtils.findMostRecentUnsuppressedBibIdFile(config, davService); 
		        		
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
		
		String mostRecentMfhdFile = FileNameUtils.findMostRecentUnsuppressedMfhdIdFile( config, davService );
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
			System.out.println("Comparing to contents of index at: " + config.getSolrUrl() );
			
			IndexRecordListComparison c = new IndexRecordListComparison();
			c.compare(config.getSolrUrl(), currentVoyagerBibList, currentVoyagerMfhdList);
			
			Integer[] bibsToDelete = c.bibsInIndexNotVoyager.toArray(new Integer[ c.bibsInIndexNotVoyager.size() ]);
			Iterator<Integer> bibidsForDeletedMFHDs = c.mfhdsInIndexNotVoyager.values().iterator();
			Integer[] bibsToAdd = c.bibsInVoyagerNotIndex.toArray(new Integer[ c.bibsInVoyagerNotIndex.size() ]);
			
			c = null; // to allow GC
			
			produceReport(bibsToDelete, bibidsForDeletedMFHDs, bibsToAdd);
		}
 	}

	

    /**
	 *  Based on the IndexRecordListComparison, BIB records to be updated and deleted written 
	 *  to a files on the WEBDAV server.
	 *  
	 *  The report files have post-pended dates in their file names.
     * @param bibsToAdd TODO
	 *  
	 */
	private void produceReport( Integer[] bibsToDelete,Iterator<Integer> bibsWithDeletedMhds, Integer[] bibsToAdd) throws Exception {

		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		
		// Write a file of BIBIDs that are in the Solr index but not voyager
		if ( bibsToDelete != null && bibsToDelete.length > 0) {
			Arrays.sort( bibsToDelete );
			StringBuilder sb = new StringBuilder();
			for( Integer id: bibsToDelete ) {
				sb.append(id);
				sb.append("\n");
			}

			String deleteReport = sb.toString(); 
			String deleteReportFile = 
			        config.getWebdavBaseUrl() + "/" + config.getDailyBibDeletes() + "/"
			        + "bibListForDelete-"+ currentDate + ".txt";			
			try {
				davService.saveFile( deleteReportFile , new ByteArrayInputStream(deleteReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + deleteReportFile);
			} catch (Exception e) {
				throw new Exception("Could not save report of deletes to '" + deleteReportFile + "'" , e);
			}		
		}

		// Write a file of BIBIDs that are in Voyager but not in the Solr index
		if ( bibsToAdd != null && bibsToAdd.length > 0) {
			Arrays.sort( bibsToAdd );
			StringBuilder sb = new StringBuilder();
			for( Integer id: bibsToAdd ) {
				sb.append(id);
				sb.append("\n");
			}

			String addReport = sb.toString();
			String addReportFile =
			        config.getWebdavBaseUrl() + "/" + config.getDailyBibAdds() + "/"
			        + "bibListToAdd-"+ currentDate + ".txt";
			try {
				davService.saveFile( addReportFile , new ByteArrayInputStream(addReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + addReportFile);
			} catch (Exception e) {
				throw new Exception("Could not save report of deletes to '" + addReportFile + "'" , e);
			}
		}
		
		// CurrentIndexMfhdList should now only contain mfhds to be deleted.
		if (bibsWithDeletedMhds != null ){			
			
			Set<Integer> update_bibids = new TreeSet<Integer>();
			while (bibsWithDeletedMhds.hasNext())
				update_bibids.add(bibsWithDeletedMhds.next());
			bibsWithDeletedMhds = update_bibids.iterator();
			StringBuilder sb = new StringBuilder();
			UPD: while (bibsWithDeletedMhds.hasNext()) {
				Integer bibid = bibsWithDeletedMhds.next();
				for (Integer id : bibsToDelete) 
					if (id.equals(bibid))
						continue UPD;
					
				sb.append(bibid);
				sb.append("\n");
			}

			String updateReport = sb.toString();

			String fileName = config.getWebdavBaseUrl() + "/" + config.getDailyBibUpdates() + "/"
			        + "bibListForUpdate-"+ currentDate + ".txt";
			try {			    
				davService.saveFile(fileName, new ByteArrayInputStream(updateReport.getBytes("UTF-8")));
				System.out.println("Wrote report to " + fileName);
			} catch (Exception e) {
			    throw new Exception("Could not save list of "
			            + "BIB IDs that need update to file '" + fileName + "'",e);   
			}
		}

		
	}
	
}
