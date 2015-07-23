package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison;

/**
 * Identify record changes from Voyager. This is done by comparing the
 * list of active BIB, MFHD and item records in Voyager along with their
 * modification dates with the similar lists in the Solr index.<br/><br/>
 *   
 * Bibs should be updated in Solr if any of the following are true:<br/>
 *  1. The set of mfhd records associated with the bib does not match.<br/>
 *  2. The set of item records associated with EACH mfhd does not match.<br/>
 *       (One possible modification involves the reassignment of an item
 *        between two mfhds for the same bib.)<br/>
 *  4. The Voyager modification dates for any single bib, mfhd or item<br/>
 *     record related to a bib is more recent than the dates in Solr.<br/><br/>
 *  
 * The reasons for deleting or adding a bib in Solr should be obvious,
 * though it's worth noting that for both bibs and holdings records, a 
 * suppressed record in Voyager is functionally equivalent to an absent
 * one.<br/><br/>
 * 
 * The lists are generated by comparing lists from step 1 (which is
 * edu.cornell.library.integration.indexer.updates.IdentifyCurrentVoyagerRecords)
 * with the contents of the Solr index. This step doesn't need to run
 * immediately after step 1, but a long delay may result in records
 * available during step 1 being unavailable during indexing, which
 * may cause the post-update index integrity check to report failure.
 */
public class IdentifyChangedRecords {
	
	DavService davService;
	SolrBuildConfig config;

	public static void main(String[] args)  {
		
		List<String> requiredArgs = new ArrayList<String>();
		requiredArgs.addAll(IndexRecordListComparison.requiredArgs());

		try{        
			new IdentifyChangedRecords( SolrBuildConfig.loadConfig( args, requiredArgs ));
		}catch( Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}	
	
	public IdentifyChangedRecords(SolrBuildConfig config) throws Exception {
	    this.config = config;
		System.out.println("Comparing to contents of index at: " + config.getSolrUrl() );

		IndexRecordListComparison c = new IndexRecordListComparison();
		c.compare(config);

/*		Integer[] bibsToDelete = c.bibsInIndexNotVoyager.toArray(new Integer[ c.bibsInIndexNotVoyager.size() ]);
		Iterator<Integer> bibsToUpdate = c.mfhdsInIndexNotVoyager.values().iterator();
		Integer[] bibsToAdd = c.bibsInVoyagerNotIndex.toArray(new Integer[ c.bibsInVoyagerNotIndex.size() ]);
*/
		
		System.out.println("Total  Bibs in Solr: "+c.bibCount);
		System.out.println("Total Mfhds in Solr: "+c.mfhdCount);
		System.out.println("Total Items in Solr: "+c.itemCount);
		Set<Integer> bibsToAdd = c.bibsInVoyagerNotIndex;
		System.out.println("Bibs To Add to Solr: "+bibsToAdd.size());
		Set<Integer> bibsToDelete = c.bibsInIndexNotVoyager;
		System.out.println("Bibs To Delete from Solr: "+bibsToDelete.size());
		
		Set<Integer> bibsToUpdate = c.bibsNewerInVoyagerThanIndex;
		bibsToUpdate.addAll(c.mfhdsNewerInVoyagerThanIndex.values());
		bibsToUpdate.addAll(c.mfhdsInIndexNotVoyager.values());
		bibsToUpdate.addAll(c.mfhdsInVoyagerNotIndex.values());
		for ( IndexRecordListComparison.ChangedBib cb : c.mfhdsAttachedToDifferentBibs.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		bibsToUpdate.addAll(c.itemsNewerInVoyagerThanIndex.values());
		bibsToUpdate.addAll(c.itemsInIndexNotVoyager.values());
		bibsToUpdate.addAll(c.itemsInVoyagerNotIndex.values());
		for ( IndexRecordListComparison.ChangedBib cb : c.itemsAttachedToDifferentMfhds.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		
		
		bibsToUpdate.removeAll(bibsToDelete);
		bibsToUpdate.removeAll(bibsToAdd);
		System.out.println("Bibs To Update in Solr: "+bibsToUpdate.size());
		
		System.out.println(String.format("\n%10d bibs updated.",c.bibsNewerInVoyagerThanIndex.size()));
		System.out.println(String.format("%10d holdings updated.",c.mfhdsNewerInVoyagerThanIndex.size()));
		System.out.println(String.format("%10d items updated.",c.itemsNewerInVoyagerThanIndex.size()));
		System.out.println(String.format("%10d mfhds added.",c.mfhdsInVoyagerNotIndex.size()));
		System.out.println(String.format("%10d items added.",c.itemsInVoyagerNotIndex.size()));
		System.out.println(String.format("%10d mfhds dropped.",c.mfhdsInIndexNotVoyager.size()));
		System.out.println(String.format("%10d items dropped.",c.itemsInIndexNotVoyager.size()));
		System.out.println(String.format("%10d mfhds reassigned.",c.mfhdsAttachedToDifferentBibs.size()));
		System.out.println(String.format("%10d mfhds reassigned.",c.itemsAttachedToDifferentMfhds.size()));
		

		c = null; // to allow GC
	
		produceReportFiles(bibsToDelete, bibsToUpdate, bibsToAdd); 
 	}

	

    /**
	 *  Based on the IndexRecordListComparison, BIB records to be updated and deleted written 
	 *  to a files on the WEBDAV server.
	 */
	private void produceReportFiles( Set<Integer> bibsToDelete,Set<Integer> bibsToUpdate, Set<Integer> bibsToAdd) throws Exception {

		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		
		// Write a file of BIBIDs that are in the Solr index but not voyager
		if ( bibsToDelete != null && bibsToDelete.size() > 0) {
			Integer[] arr = bibsToDelete.toArray(new Integer[ bibsToDelete.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
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
		if ( bibsToAdd != null && bibsToAdd.size() > 0) {
			Integer[] arr = bibsToAdd.toArray(new Integer[ bibsToAdd.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
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
				throw new Exception("Could not save report of adds to '" + addReportFile + "'" , e);
			}
		}
		
		// CurrentIndexMfhdList should now only contain mfhds to be deleted.
		if (bibsToUpdate != null && bibsToUpdate.size() > 0){			
			
			Integer[] arr = bibsToUpdate.toArray(new Integer[ bibsToUpdate.size() ]);
			Arrays.sort( arr );
			StringBuilder sb = new StringBuilder();
			for( Integer id: arr ) {
				sb.append(id);
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
