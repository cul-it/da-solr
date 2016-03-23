package edu.cornell.library.integration.indexer.updates;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison;
import edu.cornell.library.integration.indexer.utilities.IndexRecordListComparison.ChangedBib;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

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
	String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());


	public static void main(String[] args)  {
		
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForWebdav();
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Current"));
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
	    this.davService = DavServiceFactory.getDavService( config );
		System.out.println("Comparing to contents of index at: " + config.getSolrUrl() );

		IndexRecordListComparison c = new IndexRecordListComparison(config);

		Set<Integer> bibsToAdd = c.bibsInVoyagerNotIndex();
		System.out.println("Bibs To Add to Solr: "+bibsToAdd.size());
		produceAddFile( bibsToAdd );
		c.queueBibs( bibsToAdd, DataChangeUpdateType.ADD );
		Set<Integer> bibsToDelete = c.bibsInIndexNotVoyager();
		System.out.println("Bibs To Delete from Solr: "+bibsToDelete.size());
		produceDeleteFile( bibsToDelete );
		c.queueBibs( bibsToDelete, DataChangeUpdateType.DELETE );

		System.out.println("Bibs To Update:");

		Set<Integer> bibsToUpdate = c.bibsNewerInVoyagerThanIndex();
		System.out.println("\tbibsNewerInVoyagerThanIndex: "+bibsToUpdate.size());

		Set<Integer> markedBibs = c.bibsMarkedAsNeedingReindexingDueToDataChange();
		System.out.println("\tbibsMarkedAsNeedingReindexingDueToDataChange: "+markedBibs.size());
		bibsToUpdate.addAll(markedBibs);
		markedBibs.clear();

		Map<Integer,Integer> tempMap = c.mfhdsNewerInVoyagerThanIndex();
		System.out.println("\tmfhdsNewerInVoyagerThanIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsNewerInVoyagerThanIndex();
		System.out.println("\titemsNewerInVoyagerThanIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsInIndexNotVoyager();
		System.out.println("\tmfhdsInIndexNotVoyager: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.mfhdsInVoyagerNotIndex();
		System.out.println("\tmfhdsInVoyagerNotIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInIndexNotVoyager();
		System.out.println("\titemsInIndexNotVoyager: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();

		tempMap = c.itemsInVoyagerNotIndex();
		System.out.println("\titemsInVoyagerNotIndex: "+tempMap.size());
		bibsToUpdate.addAll(tempMap.values());
		tempMap.clear();
		
		Map<Integer,ChangedBib> tempCBMap = c.mfhdsAttachedToDifferentBibs();
		System.out.println("\tmfhdsAttachedToDifferentBibs: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();

		tempCBMap = c.itemsAttachedToDifferentMfhds();
		System.out.println("\titemsAttachedToDifferentMfhds: "+tempCBMap.size());
		for ( IndexRecordListComparison.ChangedBib cb : tempCBMap.values()) {
			bibsToUpdate.add(cb.original);
			bibsToUpdate.add(cb.changed);
		}
		tempCBMap.clear();

		bibsToUpdate.removeAll(bibsToDelete);
		bibsToUpdate.removeAll(bibsToAdd);
		System.out.println("Bibs To Update in Solr: "+bibsToUpdate.size());		
		produceUpdateFile(bibsToUpdate);
		c.queueBibs( bibsToUpdate, DataChangeUpdateType.UPDATE );

		c = null; // to allow GC
	
 	}

	private void produceDeleteFile( Set<Integer> bibsToDelete ) throws Exception {

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
		
	}
	
	private void produceAddFile(Set<Integer> bibsToAdd) throws Exception {

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

	}

	private void produceUpdateFile( Set<Integer> bibsToUpdate) throws Exception {

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

	public static enum DataChangeUpdateType {
		ADD("Added Record"),
		UPDATE("Record Update"),
		DELETE("Record Deleted or Suppressed"),
		TITLELINK("Title Link Update");

		private String string;

		private DataChangeUpdateType(String name) {
			string = name;
		}

		public String toString() { return string; }
	}
	
}
