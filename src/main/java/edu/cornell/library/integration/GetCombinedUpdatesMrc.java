package edu.cornell.library.integration;

import static edu.cornell.library.integration.utilities.IndexingUtilities.addBibToUpdateQueue;
import static edu.cornell.library.integration.utilities.IndexingUtilities.queueBibDelete;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.updates.IdentifyChangedRecords.DataChangeUpdateType;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;

/**
 * Retrieve a set of bib and holdings records that have been flagged as needing
 * indexing or reindexing then saves the MARC for these records to a WEBDAV directory.  
 * 
 */
public class GetCombinedUpdatesMrc extends VoyagerToSolrStep {
	
	private static Integer minUpdateBibCount = 150_000;
   
   /**
    * Main is called with the normal VoyagerToSolrConfiguration args.
    */
   public static void main(String[] args) throws Exception {
     List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
     requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));
     requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
     requiredArgs.add("dailyMrcDir");
     requiredArgs.add("dailyMfhdDir");
   
     new GetCombinedUpdatesMrc(SolrBuildConfig.loadConfig(args, requiredArgs));
   }
   
   private Connection current;
   
	public GetCombinedUpdatesMrc(SolrBuildConfig config) throws Exception{

	    current = config.getDatabaseConnection("Current");

        Set<Integer> updatedBibIds = getBibsToUpdateOrAdd( config );
        System.out.println("Updated and added bibs identified: "+updatedBibIds.size());
	    Set<Integer> suppressedBibs = checkForSuppressedRecords(current,updatedBibIds);
	    if ( ! suppressedBibs.isEmpty()) {
	    	System.out.println("suppressed bibs eliminated from list: "
	    			+suppressedBibs.size()+" ("+StringUtils.join(suppressedBibs, ", ")+")");
	    	updatedBibIds.removeAll(suppressedBibs);
	    }
	     	    
	    // Get MFHD IDs for all the BIB IDs
		System.out.println("Identifying holdings ids");
		Set<Integer> updatedMfhdIds =  getHoldingsForBibs( current, updatedBibIds );
		
		System.out.println("Total BibIDList: " + updatedBibIds.size());
		System.out.println("Total MfhdIDList: " + updatedMfhdIds.size());

		setDavService(DavServiceFactory.getDavService( config ));
		Connection voyager = config.getDatabaseConnection("Voy");
		saveBIBsToMARC(  voyager, updatedBibIds , config.getWebdavBaseUrl() + "/" + config.getDailyMrcDir() );
		saveMFHDsToMARC( voyager, updatedMfhdIds, config.getWebdavBaseUrl() + "/" + config.getDailyMfhdDir() );
	    current.close();
		voyager.close();
	}


    private Set<Integer> checkForSuppressedRecords(Connection current, Set<Integer> updatedIds) throws SQLException {
    	PreparedStatement pstmt = current.prepareStatement(
    			"SELECT * FROM "+CurrentDBTable.BIB_VOY.toString()+" WHERE bib_id = ?");
    	Set<Integer> suppressed = new HashSet<Integer>();
    	for (Integer id : updatedIds) {
    		pstmt.setInt(1, id);
    		ResultSet rs = pstmt.executeQuery();
    		boolean isSuppressed = true;
    		while (rs.next())
    			isSuppressed = false;
    		rs.close();
    		if (isSuppressed)
    			suppressed.add(id);
    	}
	    pstmt.close();
    	return suppressed;
	}

	/**
     * Get the MARC for each MFHD ID and concatenate data to create MARC files. 
     * Only put 10000 MARC records in a file.
     */
	private void saveMFHDsToMARC(Connection voyager, Set<Integer> updatedMfhdIds, String mfhdDestDir) throws Exception {
	    int recno = 0;
        int maxrec = 10000;
        int seqno = 1;
        StringBuffer sb = new StringBuffer();

        PreparedStatement mfhdStmt = voyager.prepareStatement(
        		"SELECT * FROM MFHD_DATA WHERE MFHD_DATA.MFHD_ID = ? ORDER BY MFHD_DATA.SEQNUM");

        Iterator<Integer> mfhdIds = updatedMfhdIds.iterator();
        while( mfhdIds.hasNext() ){
        	Integer mfhdid = mfhdIds.next();

        	ByteArrayOutputStream bb = new ByteArrayOutputStream(); 
            mfhdStmt.setInt(1,mfhdid);
            ResultSet rs = mfhdStmt.executeQuery();
            while (rs.next()) bb.write(rs.getBytes("RECORD_SEGMENT"));
            rs.close();
            if (bb.size() == 0) {
            	System.out.println("Skipping record m"+mfhdid+". Could not retrieve from Voyager. If available, the bib will still be indexed.");
            	continue;
            }

            String marcRecord = new String( bb.toByteArray(), StandardCharsets.UTF_8 );

            if ( marcRecord.contains("\uFFFD") ) {
            	System.out.println("Mfhd MARC contains Unicode Replacement Character (U+FFFD): "+mfhdid);
            	System.out.println(marcRecord);
            }

            sb.append(marcRecord);
            /* Inserting a carriage return after each MARC record in the file.
             * This is not valid in a technically correct MARC "database" file, but
             * is supported by org.marc4j.MarcPermissiveStreamReader. If we ever stop using this
             * library, we may need to remove this. For now, it simplifies pulling problem
             * records from the MARC "database".
             */
            sb.append('\n');

            recno = recno + 1;
            if (recno >= maxrec) {         
            	saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
                seqno = seqno + 1;
                recno = 0; 
                sb = new StringBuffer();                             
            }                            
        }
        if (recno != 0)
        	saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
        mfhdStmt.close();
    }


    /**
	 * Get the MARC for each BIB ID, concatenate bib data to create MARC
	 * files. Only put 10000 MARC records in a file.
	 */
	private void saveBIBsToMARC(Connection voyager, Set<Integer> updatedBibIds, String bibDestDir) throws Exception {
        int recno = 0;
        int maxrec = 10000;
        int seqno = 1;
        StringBuffer sb = new StringBuffer();

        PreparedStatement bibStmt = voyager.prepareStatement(
        		"SELECT * FROM BIB_DATA WHERE BIB_DATA.BIB_ID = ? ORDER BY BIB_DATA.SEQNUM");

        Iterator<Integer> bibIds = updatedBibIds.iterator();
        while( bibIds.hasNext()){
        	Integer bibid  = bibIds.next();

        	ByteArrayOutputStream bb = new ByteArrayOutputStream(); 
        	bibStmt.setInt(1, bibid);
            ResultSet rs = bibStmt.executeQuery();
            while (rs.next()) bb.write(rs.getBytes("RECORD_SEGMENT"));
            rs.close();
            if (bb.size() == 0) {
            	System.out.println("Skipping record b"+bibid+". Could not retrieve from Voyager.");
				queueBibDelete( current, bibid );
				continue;
            }

            String marcRecord = new String( bb.toByteArray(), StandardCharsets.UTF_8 );

            if ( marcRecord.contains("\uFFFD") ) {
            	System.out.println("Bib MARC contains Unicode Replacement Character (U+FFFD): "+bibid);
            	System.out.println(marcRecord);
            }

            sb.append(marcRecord);
            /* Inserting a carriage return after each MARC record in the file.
             * This is not valid in a technically correct MARC "database" file, but
             * is supported by org.marc4j.MarcPermissiveStreamReader. If we ever stop using this
             * library, we may need to remove this. For now, it simplifies pulling problem
             * records from the MARC "database".
             */
            sb.append('\n');

            recno = recno + 1;
            if (recno >= maxrec) {
            	saveBibMrc(sb.toString(), seqno, bibDestDir);
            	recno = 0;
            	seqno = seqno + 1;
            	sb = new StringBuffer();
            }
        }
        if (recno != 0)
        	saveBibMrc(sb.toString(), seqno, bibDestDir);
        bibStmt.close();
    }


    /**
	 * Gets the MFHD IDs related to all the BIB IDs in bibIds 
	 * @throws Exception 
	 */
	private Set<Integer> getHoldingsForBibs( Connection current, Set<Integer> bibIds) throws Exception {
		PreparedStatement holdingsForBibsStmt = current.prepareStatement(
				"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_VOY.toString()+" WHERE bib_id = ?");
        Set<Integer> newMfhdIdSet = new HashSet<Integer>();        
        for (Integer bibid : bibIds) {
        	holdingsForBibsStmt.setInt(1,bibid);
        	ResultSet rs = holdingsForBibsStmt.executeQuery();
        	while (rs.next())
        		newMfhdIdSet.add(rs.getInt(1));
        	rs.close();
        }
        holdingsForBibsStmt.close();
        return newMfhdIdSet;
	}	


    /**
	 * Gets the list of BIB IDs that are new or newly unsuppressed (or otherwise missing
	 * from the index). This list was generated in step 2.
	 * @throws Exception 
	 * 
	 */
	private Set<Integer> getBibsToUpdateOrAdd( SolrBuildConfig config ) throws Exception {

        Integer configRecCount = config.getTargetDailyUpdatesBibCount();
        if (configRecCount != null) {
        	System.out.println("Target updates bib count set to "+configRecCount);
        	minUpdateBibCount = configRecCount;
        }

        Set<Integer> addedBibs = new HashSet<Integer>(minUpdateBibCount);

        PreparedStatement pstmt = current.prepareStatement(
			"SELECT * FROM "+CurrentDBTable.QUEUE
			+" WHERE done_date = 0"
			+" ORDER BY priority"
			+" LIMIT " + Math.round(minUpdateBibCount*1.125));
        ResultSet rs = pstmt.executeQuery();
        final String delete = DataChangeUpdateType.DELETE.toString();

        while (rs.next() && addedBibs.size() < minUpdateBibCount) {
        	if (rs.getString("cause").equals(delete))
        		continue;
        	addedBibs.add(rs.getInt("bib_id"));
        }
        rs.close();
        pstmt.close();

        if (addedBibs.size() < minUpdateBibCount) {
            HttpSolrServer solr = new HttpSolrServer(config.getSolrUrl());
            SolrQuery query = new SolrQuery();
            query.setQuery("*:*");
            query.setSort("timestamp", ORDER.asc);
            query.setFields("id");
            query.setRows(minUpdateBibCount - addedBibs.size());
            for (SolrDocument doc : solr.query(query).getResults()) {
            	int bib_id = Integer.valueOf(
                        doc.getFieldValues("id").iterator().next().toString());
            	if ( ! addedBibs.contains(bib_id) ) {
            		addedBibs.add(bib_id);
            		addBibToUpdateQueue(current, bib_id, DataChangeUpdateType.AGE_IN_SOLR);
            	}
            		
            }
        }
        return addedBibs;
    }

    private void saveBibMrc(String mrc, int seqno, String destDir)
			throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/bib.update." + getDateString(now) + "."+ seqno +".mrc";
		System.out.println("Saving BIB mrc to "+ url);
		InputStream isr = IOUtils.toInputStream(mrc, StandardCharsets.UTF_8);
		getDavService().saveFile(url, isr);
	}
	
	private void saveMfhdMrc(String mrc, int seqno, String destDir) throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/mfhd.update." + getDateString(now) + "."+ seqno +".mrc";
		System.out.println("Saving MFHD mrc to: "+ url);
		InputStream isr = IOUtils.toInputStream(mrc, StandardCharsets.UTF_8);
		getDavService().saveFile(url, isr);
	}
   
   private String getDateString(Calendar cal) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
	   String ds = df.format(cal.getTime());
	   return ds;
   }
       
}
