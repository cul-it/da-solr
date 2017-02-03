package edu.cornell.library.integration.indexer.updates;

import static edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.getBibsToIndex;
import static edu.cornell.library.integration.utilities.IndexingUtilities.getHoldingsForBibs;
import static edu.cornell.library.integration.utilities.IndexingUtilities.queueBibDelete;

import java.sql.Connection;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.indexer.BatchRecordsForSolrIndex.BatchLogic;
import edu.cornell.library.integration.indexer.MarcRecord.RecordType;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;
import edu.cornell.library.integration.voyager.DownloadMARC;

public class RetrieveUpdatesBatch {

	
	public RetrieveUpdatesBatch(SolrBuildConfig config, BatchLogic b) throws Exception {

		try ( Connection current = config.getDatabaseConnection("Current")) {
			Integer batchsize = config.getTargetBatchSize();
	        if (batchsize != null) {
	        	System.out.println("Target updates bib count set to "+batchsize);
	        } else {
	        	batchsize = 1_000;
	        	System.out.println("Target batch size not found, defaulting to "+batchsize);
	        }
	        Set<Integer> bibIds = identifyBibBatch(current,b);
	        Set<Integer> mfhdIds = getHoldingsForBibs(current,bibIds);

	        DownloadMARC downloader = new DownloadMARC(config);
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

		}

		DavService davService = DavServiceFactory.getDavService(config);
		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.RECORD_COUNT_BATCHES);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(), davService);
		converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir(),davService);
		converter.setDestFilenamePrefix( config.getDailyMrcNtFilenamePrefix() );
		converter.run();

	}

	private static Set<Integer> identifyBibBatch (Connection current, BatchLogic b) throws Exception {
		Set<Integer> bibIds = getBibsToIndex(current,b);
		return bibIds;
	}

}
