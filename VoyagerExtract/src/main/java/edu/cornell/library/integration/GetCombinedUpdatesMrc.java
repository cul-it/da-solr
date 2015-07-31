package edu.cornell.library.integration;

import java.io.File;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import static edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig.getRequiredArgsForWebdav;

/**
 * This gets a list of BIB and MFHD records updates from the Voyager database for a
 * given period of time and then saves the MARC for these records to a WEBDAV directory.  
 * 
 * This list is combined with the list of BIB records that have had their MHFD 
 * records deleted which is generated by IdentifyDeletedRecords.
 * 
 * Once the list of the IDs of BIB and MFHD records that are needed is gathered,
 * the Voyager database is queries to get the MARC.  That data is saved to a 
 * sequence of files.
 */
public class GetCombinedUpdatesMrc extends VoyagerToSolrStep {
   
   /**
    * default constructor
    */
   public GetCombinedUpdatesMrc() { 
       
   }     

   /**
    * Main is called with the normal VoyagerToSolrConfiguration args.
    */
   public static void main(String[] args) throws Exception {                
     GetCombinedUpdatesMrc app = new GetCombinedUpdatesMrc();
     List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
     requiredArgs.addAll(getRequiredArgsForWebdav());
     requiredArgs.add("dailyMrcDir");
     requiredArgs.add("dailyMfhdDir");
     requiredArgs.add("dailyBibUpdates");
     requiredArgs.add("dailyBibAdds");
     
     app.getCombinedUpatedsAndSaveAsMARC( SolrBuildConfig.loadConfig(args, requiredArgs) );     
   }
   
   private Connection current;
   
	private void getCombinedUpatedsAndSaveAsMARC(SolrBuildConfig config) 
	        throws Exception{

		if ( getCatalogService() == null )
		    throw new Exception("Could not get catalogService");			
		
		setDavService(DavServiceFactory.getDavService( config ));

        Set<String> updatedBibIds = new HashSet<String>();

		String date =  getDateString(Calendar.getInstance());
		Set<String> bibListForUpdate = getUpdatedBibs( config, date );
	    System.out.println("bibListForUpdate: " + bibListForUpdate.size() );
	    updatedBibIds.addAll(bibListForUpdate);
		Set<String> bibListForAdd = getBibIdsToAdd( config, date );
	    System.out.println("bibListForAdd: " + bibListForAdd.size() );
	    updatedBibIds.addAll( bibListForAdd );
	    current = config.getDatabaseConnection("Current");
	    Set<String> suppressedBibs = checkForSuppressedBibs(updatedBibIds);
	    if ( ! suppressedBibs.isEmpty()) {
	    	System.out.println("suppressed bibs eliminated from list: "+suppressedBibs.size());
	    	updatedBibIds.removeAll(suppressedBibs);
	    }
	     	    
	    // Get MFHD IDs for all the BIB IDs
		System.out.println("Identifying holdings ids");
		Set<String> updatedMfhdIds =  getHoldingsForBibs( updatedBibIds );
	    Set<String> suppressedMfhds = checkForSuppressedMfhds(updatedMfhdIds);
	    if ( ! suppressedMfhds.isEmpty()) {
	    	updatedMfhdIds.removeAll(suppressedMfhds);
	    }
		
		System.out.println("Total BibIDList: " + updatedBibIds.size());
		System.out.println("Total MfhdIDList: " + updatedMfhdIds.size());

		saveBIBsToMARC(  updatedBibIds , config.getWebdavBaseUrl() + "/" + config.getDailyMrcDir() );
		saveMFHDsToMARC( updatedMfhdIds, config.getWebdavBaseUrl() + "/" + config.getDailyMfhdDir() );
	}


    private Set<String> checkForSuppressedMfhds(Set<String> updatedMfhdIds) throws SQLException {
    	Set<String> suppressed = new HashSet<String>();
    	String mfhdTable = "mfhd_"+new SimpleDateFormat("yyyyMMdd").
    			format(Calendar.getInstance().getTime());
    	PreparedStatement pstmt = current.prepareStatement(
    			"SELECT * FROM "+mfhdTable+" WHERE mfhd_id = ?");
    	for (String mfhd_id : updatedMfhdIds) {
    		pstmt.setInt(1, Integer.valueOf(mfhd_id));
    		ResultSet rs = pstmt.executeQuery();
    		boolean mfhdSuppressed = true;
    		while (rs.next())
    			mfhdSuppressed = false;
    		rs.close();
    		if (mfhdSuppressed)
    			suppressed.add(mfhd_id);
    	}
    	return suppressed;
	}

	private Set<String> checkForSuppressedBibs(Set<String> updatedBibIds) throws SQLException {
    	Set<String> suppressed = new HashSet<String>();
    	String bibTable = "bib_"+new SimpleDateFormat("yyyyMMdd").
    			format(Calendar.getInstance().getTime());
    	PreparedStatement pstmt = current.prepareStatement(
    			"SELECT * FROM "+bibTable+" WHERE bib_id = ?");
    	for (String bib_id : updatedBibIds) {
    		pstmt.setInt(1, Integer.valueOf(bib_id));
    		ResultSet rs = pstmt.executeQuery();
    		boolean bibSuppressed = true;
    		while (rs.next())
    			bibSuppressed = false;
    		rs.close();
    		if (bibSuppressed)
    			suppressed.add(bib_id);
    	}
    	return suppressed;
	}

	/**
     * Get the MARC for each MFHD ID and concatenate data to create MARC files. 
     * Only put 10000 MARC records in a file.
     */
	private void saveMFHDsToMARC(Set<String> updatedMfhdIds, String mfhdDestDir) throws Exception {
	    int recno = 0;
        int maxrec = 10000;
        int seqno = 1;
        StringBuffer sb = new StringBuffer();                        
        
        Iterator<String> mfhdIds = updatedMfhdIds.iterator();
        while( mfhdIds.hasNext() ){
            String mfhdid = mfhdIds.next();
            
            List<MfhdData> mfhdDataList;
            try {
                mfhdDataList = getCatalogService().getMfhdData(mfhdid);
            } catch (Exception e) { 
                throw new Exception("Could not get MARC for MFHD ID " + mfhdid, e );
            } 
            
            for (MfhdData mfhdData : mfhdDataList) { 
                sb.append(mfhdData.getRecord()); 
            }
            /* Inserting a carriage return after each MARC record in the file.
             * This is not valid in a technically correct MARC "database" file, but
             * is supported by org.marc4j.MarcPermissiveStreamReader. If we ever stop using this
             * library, we may need to remove this. For now, it simplifies pulling problem
             * records from the MARC "database".
             */
            if (! mfhdDataList.isEmpty())
         	   sb.append('\n');
            
            recno = recno + 1;
            
            if (recno >= maxrec || ! mfhdIds.hasNext() ) {         
                try {                   
                    saveMfhdMrc(sb.toString(), seqno, mfhdDestDir);
                } catch (Exception e) {
                    throw new Exception("Problem saving MFHD MARC to a WEBDAV file. ", e);
                }  
                seqno = seqno + 1;
                recno = 0; 
                sb = new StringBuffer();                             
            }                            
        }

    }


    /**
	 * Get the MARC for each BIB ID, concatenate bib data to create MARC
	 * files. Only put 10000 MARC records in a file.
	 */
	private void saveBIBsToMARC(Set<String> updatedBibIds, String bibDestDir) throws Exception {
        int recno = 0;
        int maxrec = 10000;
        int seqno = 1;
        StringBuffer sb = new StringBuffer();
        
        Iterator<String> bibIds = updatedBibIds.iterator();
        while( bibIds.hasNext()){
            String bibid  = bibIds.next();
            
            List<BibData> bibDataList;
            try {
               bibDataList = getCatalogService().getBibData(bibid);
            } catch (Exception e) { 
                throw new Exception("Could not get MARC for BIB ID " + bibid, e);
            } 
               
           for (BibData bibData : bibDataList) { 
               sb.append(bibData.getRecord()); 
           }
           /* Inserting a carriage return after each MARC record in the file.
            * This is not valid in a technically correct MARC "database" file, but
            * is supported by org.marc4j.MarcPermissiveStreamReader. If we ever stop using this
            * library, we may need to remove this. For now, it simplifies pulling problem
            * records from the MARC "database".
            */
           if (! bibDataList.isEmpty())
        	   sb.append('\n');
           
           recno = recno + 1;
           if (recno >= maxrec || ! bibIds.hasNext() ) {
               try{
                   saveBibMrc(sb.toString(), seqno, bibDestDir);
               } catch (Exception e) {
                   throw new Exception("Problem saving BIB MARC to a WEBDAV file. ", e);
               }
               recno = 0;
               seqno = seqno + 1;
               sb = new StringBuffer();
           }                           
        }                               
    }


    /**
	 * Gets the MFHD IDs related to all the BIB IDs in bibIds 
	 * @throws Exception 
	 */
	private Set<String> getHoldingsForBibs( Set<String> bibIds) throws Exception {	    	    
        Set<String> newMfhdIdSet = new HashSet<String>();        
        for (String bibid : bibIds) {
            try {
                newMfhdIdSet.addAll( getCatalogService().getMfhdIdsByBibId(bibid) );                                    
            } catch (Exception e) {
                throw new Exception("Could not get the MFHD IDs for BIB ID " + bibid, e);
            } 
        }         
        return newMfhdIdSet;
	}	


    /**
	 * Gets the list of BIB IDs require updating in Solr.
	 * @throws Exception 
	 * 
	 */
	private Set<String> getUpdatedBibs( SolrBuildConfig config, String today ) throws Exception {

        List<String> updateFiles = getDavService().getFileUrlList(
        		config.getWebdavBaseUrl() + "/" + config.getDailyBibUpdates() + "/");
        Set<String> updatedBibs = new HashSet<String>();
        for (String url : updateFiles) {
    	    final Path tempPath = Files.createTempFile("getCombinedUpdatesMrc-", ".txt");
    		tempPath.toFile().deleteOnExit();
            File localTmpFile = getDavService().getFile(url, tempPath.toString());
            updatedBibs.addAll(FileUtils.readLines(localTmpFile));
            localTmpFile.delete();
        }
        return updatedBibs;
    }


    /**
	 * Gets the list of BIB IDs that are new or newly unsuppressed (or otherwise missing
	 * from the index). This list was generated in step 2.
	 * @throws Exception 
	 * 
	 */
	private Set<String> getBibIdsToAdd( SolrBuildConfig config, String today ) throws Exception {
        
        List<String> addFiles = getDavService().getFileUrlList(
        		config.getWebdavBaseUrl() + "/" + config.getDailyBibAdds() + "/");
        Set<String> addedBibs = new HashSet<String>();
        for (String url : addFiles) {
    	    final Path tempPath = Files.createTempFile("getCombinedUpdatesMrc-", ".txt");
    		tempPath.toFile().deleteOnExit();
            File localTmpFile = getDavService().getFile(url, tempPath.toString());
            addedBibs.addAll(FileUtils.readLines(localTmpFile));
            localTmpFile.delete();
        }
        return addedBibs;
    }

    private void saveBibMrc(String mrc, int seqno, String destDir)
			throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/bib.update." + getDateString(now) + "."+ seqno +".mrc";
		System.out.println("Saving BIB mrc to "+ url);
		try { 
			InputStream isr = IOUtils.toInputStream(mrc, "UTF-8");
			getDavService().saveFile(url, isr);

		} catch (UnsupportedEncodingException ex) {
			throw ex;
		} catch (Exception ex) {
			throw ex;
		}
	}
	
	private void saveMfhdMrc(String mrc, int seqno, String destDir)	throws Exception {
		Calendar now = Calendar.getInstance();
		String url = destDir + "/mfhd.update." + getDateString(now) + "."+ seqno +".mrc";
		System.out.println("Saving MFHD mrc to: "+ url);
		try {
			InputStream isr = IOUtils.toInputStream(mrc, "UTF-8");
			getDavService().saveFile(url, isr);

		} catch (UnsupportedEncodingException ex) {
			throw ex;
		} catch (Exception ex) {
			throw ex;
		}
	}
   
   private String getDateString(Calendar cal) {
	   SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
	   String ds = df.format(cal.getTime());
	   return ds;
   }
       
}
