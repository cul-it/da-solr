package edu.cornell.library.integration.indexer.updates;

import java.util.Calendar;
import java.util.List;

import edu.cornell.library.integration.ConvertMarcToXml;
import edu.cornell.library.integration.GetCombinedUpdatesMrc;
import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;

public class IterativeUpdateFromVoyager {
	public static void main(String[] args) throws Exception {
		List<String> requiredArgs = SolrBuildConfig.getRequiredArgsForDB("Current");
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForDB("Voy"));
		requiredArgs.addAll(SolrBuildConfig.getRequiredArgsForWebdav());
		requiredArgs.add("dailyMrcDir");
		requiredArgs.add("dailyMfhdDir");
		requiredArgs.add("marc2XmlDirs");
		requiredArgs.add("dailyBibMrcXmlDir");
		requiredArgs.add("dailyMfhdMrcXmlDir");
		requiredArgs.add("dailyMrcNtDir");
		requiredArgs.add("dailyMrcNtFilenamePrefix");
    	requiredArgs.add("solrUrl");
		SolrBuildConfig config = SolrBuildConfig.loadConfig(args,requiredArgs);

		switch (Calendar.getInstance().get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SATURDAY:
		case Calendar.SUNDAY:
			config.setTargetBatchSize(5_000); break;
		default: config.setTargetBatchSize(1_000);
		}

		String webdavBaseURL = config.getWebdavBaseUrl();
		String localBaseFilePath = config.getLocalBaseFilePath();
		config.setDatabasePoolsize("Current", 2);
		int quittingTime = ( config.getExtendedIndexingMode() ) ? 23 : 18;
		System.out.println("Processing updates to Voyager until: "+quittingTime+":00.");

		int i = 0;
		while (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) < quittingTime) {

			config.setWebdavBaseUrl(webdavBaseURL + "/" + (++i) );
			config.setLocalBaseFilePath(localBaseFilePath + "/" + i);
			new IdentifyChangedRecords(config,false);
			DeleteFromSolr dfs = new DeleteFromSolr();
			dfs.doTheDelete(config);
			new GetCombinedUpdatesMrc(config);
			new ConvertMarcToXml(config);

			DavService davService = DavServiceFactory.getDavService(config);
			MarcXmlToRdf converter = new MarcXmlToRdf(Mode.RECORD_COUNT_BATCHES);
			converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(), davService);
			converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(), davService);
			converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir(),davService);
			converter.setDestFilenamePrefix( config.getDailyMrcNtFilenamePrefix() );
			converter.run();

			new IncrementalBibFileToSolr(config);

		}
	}

}
