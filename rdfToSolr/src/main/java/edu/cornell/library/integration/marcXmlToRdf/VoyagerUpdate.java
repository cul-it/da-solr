package edu.cornell.library.integration.marcXmlToRdf;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import edu.cornell.library.integration.ilcommons.configuration.SolrBuildConfig;
import edu.cornell.library.integration.ilcommons.service.DavService;
import edu.cornell.library.integration.ilcommons.service.DavServiceFactory;
import edu.cornell.library.integration.marcXmlToRdf.MarcXmlToRdf.Mode;

public class VoyagerUpdate {
	
	DavService davService;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {		
	    new VoyagerUpdate(args);			
	}
	
	public VoyagerUpdate(String[] args) throws Exception {
		
		Collection<String> requiredFields = new HashSet<String>();
		requiredFields.add("dailyBibMrcXmlDir");
		requiredFields.add("dailyMfhdMrcXmlDir");
		requiredFields.add("dailyMrcNtDir");
		requiredFields.add("dailyMrcNtFilenamePrefix");
		SolrBuildConfig config =
				SolrBuildConfig.loadConfig( args, requiredFields );
		
		davService = DavServiceFactory.getDavService(config);				
		String currentDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
					    
		MarcXmlToRdf converter = new MarcXmlToRdf(Mode.RECORD_COUNT_BATCHES);
		converter.setBibSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyBibMrcXmlDir(), davService);
		converter.setMfhdSrcDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMfhdMrcXmlDir(), davService);
		converter.setDestDavDir(config.getWebdavBaseUrl() + "/" + config.getDailyMrcNtDir(),davService);
		converter.setDestFilenamePrefix( config.getDailyMrcNtFilenamePrefix() + "-" + currentDate );
		converter.run();

	}

}
