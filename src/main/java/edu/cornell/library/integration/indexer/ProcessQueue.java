package edu.cornell.library.integration.indexer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.MarcRecord.RecordType;
import edu.cornell.library.integration.indexer.updates.IncrementalBibFileToSolr;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.utilities.DaSolrUtilities.CurrentDBTable;
import edu.cornell.library.integration.voyager.DownloadMARC;

import static edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.getBibsToIndex;
import static edu.cornell.library.integration.utilities.IndexingUtilities.queueBibDelete;

public class ProcessQueue {

	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("solrUrl");
		requiredArgs.add("dailyBibMrcXmlDir");
		requiredArgs.add("dailyMfhdMrcXmlDir");
		requiredArgs.add("dailyMrcNtDir");
		requiredArgs.add("dailyMrcNtFilenamePrefix");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(args, requiredArgs);

		new ProcessQueue(config);
	}

	public ProcessQueue(SolrBuildConfig config) throws Exception {
		int batchSize = 5_000;

		String random = RandomStringUtils.randomAlphanumeric(12);
		String webdavBaseUrl = config.getWebdavBaseUrl()+"/"+random+"/";
		String localBaseFilePath = config.getLocalBaseFilePath();
		if (localBaseFilePath != null)
			localBaseFilePath += "/"+random+"/";
		DownloadMARC downloader = new DownloadMARC(config);
		
		Connection current = config.getDatabaseConnection("Current");
		int i = 0;
		while (true) {
			config.setWebdavBaseUrl(webdavBaseUrl+(++i));
			if (localBaseFilePath != null)
				config.setLocalBaseFilePath(localBaseFilePath+i);
			Set<Integer> bibIds = getBibsToIndex(current,config.getSolrUrl(),0,batchSize);
			Set<Integer> mfhdIds = getHoldingsForBibs(current,bibIds);

			Set<Integer> deletedBibs = downloader.saveXml(
					RecordType.BIBLIOGRAPHIC, bibIds, config.getDailyBibMrcXmlDir());
			Set<Integer> deletedMfhds = downloader.saveXml(
					RecordType.HOLDINGS, mfhdIds, config.getDailyMfhdMrcXmlDir());
			if ( ! deletedBibs.isEmpty() ) {
				System.out.println(deletedBibs.size()+" bibs queued in this batch appear to"
						+ " have been deleted from Voyager and will be removed from the queue,"
						+ " and from Solr if applicable. "+StringUtils.join(deletedBibs, ", "));
				for (int id : deletedBibs)
					queueBibDelete( current, id );
			}
			if ( ! deletedMfhds.isEmpty() ) {
				System.out.println(deletedMfhds.size()+ " holdings records queued in this batch"
						+ " appear to have been deleted from Voyager. Their bibs, if still present,"
						+ " will be (re)indexed without their data. "+StringUtils.join(deletedMfhds,", "));
			}

			DavService davService = DavServiceFactory.getDavService(config);
			MarcXmlToRdf converter = new MarcXmlToRdf(Mode.RECORD_COUNT_BATCHES);
			converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(), davService);
			converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(), davService);
			converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir(),davService);
			converter.setDestFilenamePrefix( config.getDailyMrcNtFilenamePrefix() );
			converter.run();

			new IncrementalBibFileToSolr(config);

			if (bibIds.size() < batchSize)
				continue;
		}
	}
	
    /**
	 * Gets the MFHD IDs related to all the BIB IDs in bibIds 
	 * @throws Exception 
	 */
	private Set<Integer> getHoldingsForBibs( Connection current, Set<Integer> bibIds) throws Exception {
		PreparedStatement pstmt = current.prepareStatement(
				"SELECT mfhd_id FROM "+CurrentDBTable.MFHD_VOY.toString()+" WHERE bib_id = ?");
        Set<Integer> mfhdIds = new HashSet<Integer>();        
        for (Integer bibid : bibIds) {
        	pstmt.setInt(1,bibid);
        	ResultSet rs = pstmt.executeQuery();
        	while (rs.next())
        		mfhdIds.add(rs.getInt(1));
        	rs.close();
        }
        pstmt.close();
        return mfhdIds;
	}	

}
