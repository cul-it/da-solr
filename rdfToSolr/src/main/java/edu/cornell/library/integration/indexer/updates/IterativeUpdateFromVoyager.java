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
	
	private final static Integer ITERATIVE_BATCH_SIZE = 1000;

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
		config.setTargetDailyUpdatesBibCount(ITERATIVE_BATCH_SIZE);

		String webdavBaseURL = config.getWebdavBaseUrl();

		int i = 0;
		while (Calendar.getInstance().get(Calendar.HOUR_OF_DAY) < 17) {
			// do the thing
			config.setWebdavBaseUrl(webdavBaseURL + "/" + (++i) );
			new IdentifyChangedRecords(config,false);
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
