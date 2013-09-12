package edu.cornell.library.integration.service;

import java.util.List;

import edu.cornell.library.integration.bo.AuthData;
import edu.cornell.library.integration.bo.BibBlob;
import edu.cornell.library.integration.bo.BibData;
import edu.cornell.library.integration.bo.BibMasterData;
import edu.cornell.library.integration.bo.Location;
import edu.cornell.library.integration.bo.MfhdBlob;
import edu.cornell.library.integration.bo.MfhdData;
import edu.cornell.library.integration.bo.MfhdMasterData;

/**
 * @author jaf30
 * 
 */
public interface CatalogService {

	/**
	 * @return
	 * @throws Exception
	 */
	public List<Location> getAllLocation() throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getRecentBibIds(String dateString) throws Exception;

	/**
	 * @param fromDate
	 * @param toDate
	 * @return
	 * @throws Exception
	 */
	public List<String> getUpdatedBibIdsUsingDateRange(String fromDate,
			String toDate) throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getRecentMfhdIds(String dateString) throws Exception;

	/**
	 * @param fromDate
	 * @param toDate
	 * @return
	 * @throws Exception
	 */
	public List<String> getUpdatedMfhdIdsUsingDateRange(String fromDate,
			String toDate) throws Exception;
	
	public List<String> getMfhdIdsByBibId(String bibid) throws Exception;
	
	public List<String> getBibIdsByMfhdId(String mfhdid) throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getRecentAuthIds(String dateString) throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public int getRecentBibIdCount(String dateString) throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public int getRecentMfhdIdCount(String dateString) throws Exception;

	/**
	 * @param bibid
	 * @return
	 * @throws Exception
	 */
	public List<BibData> getBibData(String bibid) throws Exception;

	/**
	 * @param mfhdid
	 * @return
	 * @throws Exception
	 */
	public List<MfhdData> getMfhdData(String mfhdid) throws Exception;

	/**
	 * @param authid
	 * @return
	 * @throws Exception
	 */
	public List<AuthData> getAuthData(String authid) throws Exception;

	/**
	 * @param bibid
	 * @return
	 * @throws Exception
	 */
	public BibBlob getBibBlob(String bibid) throws Exception;

	/**
	 * @param mfhdid
	 * @return
	 * @throws Exception
	 */
	public MfhdBlob getMfhdBlob(String mfhdid) throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllSuppressedBibId() throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllUnSuppressedBibId() throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllSuppressedMfhdId() throws Exception;

	/**
	 * @return
	 * @throws Exception
	 */
	public List<String> getAllUnSuppressedMfhdId() throws Exception;

	/**
	 * @param fromDateString
	 * @param toDateString
	 * @return
	 * @throws Exception
	 */
	public List<String> getSuppressedBibId(String fromDateString,
			String toDateString) throws Exception;

	/**
	 * @param fromDateString
	 * @param toDateString
	 * @return
	 * @throws Exception
	 */
	public List<String> getSuppressedMfhdId(String fromDateString,
			String toDateString) throws Exception;

	/**
	 * @param bibid
	 * @return
	 * @throws Exception
	 */
	public BibMasterData getBibMasterData(String bibid) throws Exception;

	/**
	 * @param mfhdid
	 * @return
	 * @throws Exception
	 */
	public MfhdMasterData getMfhdMasterData(String mfhdid) throws Exception;

}
